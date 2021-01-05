#
# Copyright 2020 Two Sigma Open Source, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import datetime as dt
from typing import Callable, Optional, Set, Tuple

from uberjob._execution.run_function_on_graph import run_function_on_graph
from uberjob._plan import Plan
from uberjob._registry import Registry, RegistryValue
from uberjob._transformations import get_mutable_plan
from uberjob._transformations.pruning import prune_plan, prune_source_literals
from uberjob._util import Slot, safe_max
from uberjob.graph import Call, Dependency, KeywordArg, Node, PositionalArg, get_scope


class BarrierType:
    __slots__ = ()

    def __repr__(self):
        return "Barrier"


Barrier = BarrierType()


def _to_naive_local_timezone(value: Optional[dt.datetime]) -> Optional[dt.datetime]:
    return value.astimezone().replace(tzinfo=None) if value and value.tzinfo else value


def _get_stale_nodes(
    plan: Plan,
    registry: Registry,
    *,
    retry,
    max_workers: Optional[int] = None,
    fresh_time: Optional[dt.datetime] = None,
    on_started: Optional[Callable[[Node], None]] = None,
    on_completed: Optional[Callable[[Node], None]] = None
) -> Set[Node]:
    plan = prune_source_literals(
        plan, inplace=False, predicate=lambda node: node not in registry
    )
    fresh_time = _to_naive_local_timezone(fresh_time)
    stale_lookup = {node: Slot(False) for node in plan.graph.nodes()}
    modified_time_lookup = {node: Slot() for node in plan.graph.nodes()}

    def process_no_stale_ancestor(node):
        max_ancestor_modified_time = safe_max(
            modified_time_lookup[predecessor].value
            for predecessor in plan.graph.predecessors(node)
        )
        value_store = registry.get(node)
        if value_store is None:
            modified_time_lookup[node].value = max_ancestor_modified_time
            return
        modified_time = _to_naive_local_timezone(retry(value_store.get_modified_time)())
        if modified_time is None:
            stale_lookup[node].value = True
            return
        if (
            max_ancestor_modified_time or not registry.mapping[node].is_source
        ) and safe_max(
            modified_time, max_ancestor_modified_time, fresh_time
        ) > modified_time:
            stale_lookup[node].value = True
            return
        modified_time_lookup[node].value = modified_time

    def process(node):
        has_stale_ancestor = any(
            stale_lookup[predecessor].value
            for predecessor in plan.graph.predecessors(node)
        )
        if has_stale_ancestor:
            stale_lookup[node].value = True
        else:
            process_no_stale_ancestor(node)

    def process_with_callbacks(node):
        if on_started is not None and type(node) is Call:
            on_started(node)
        try:
            process(node)
        finally:
            if on_completed is not None and type(node) is Call:
                on_completed(node)

    run_function_on_graph(
        plan.graph, process_with_callbacks, worker_count=max_workers, scheduler="cheap"
    )
    return {k for k, v in stale_lookup.items() if v.value}


def _add_value_store(
    plan: Plan, node: Node, registry_value: RegistryValue, *, is_stale: bool
) -> Tuple[Optional[Node], Node]:
    def nested_call(*args):
        call = plan._call(registry_value.stack_frame, *args)
        call_data = plan.graph.nodes[call]
        call_data["implicit_scope"] = (
            plan.graph.nodes[node].get("implicit_scope", ())
            + call_data["implicit_scope"]
        )
        return call

    out_edges = list(plan.graph.out_edges(node, keys=True))
    value_store = registry_value.value_store

    with plan.scope(*get_scope(plan.graph, node)):
        value_store_lit = plan.lit(value_store)
        write_node = None
        read_node = nested_call(value_store.__class__.read, value_store_lit)
        if is_stale:
            if registry_value.is_source:
                write_node = plan.lit(Barrier)
                for predecessor in plan.graph.predecessors(node):
                    plan.graph.add_edge(predecessor, write_node, Dependency())
            else:
                write_node = nested_call(
                    value_store.__class__.write, value_store_lit, node
                )
            plan.graph.add_edge(write_node, read_node, Dependency())

    for _, successor, dependency in out_edges:
        plan.graph.remove_edge(node, successor, dependency)
        dependency_type = type(dependency)
        if dependency_type in (PositionalArg, KeywordArg):
            plan.graph.add_edge(read_node, successor, dependency)
        elif is_stale:
            assert dependency_type is Dependency
            plan.graph.add_edge(write_node, successor, dependency)

    return write_node, read_node


def plan_with_value_stores(
    plan: Plan,
    registry: Registry,
    *,
    output_node: Optional[Node],
    max_workers: Optional[int] = None,
    retry,
    fresh_time: Optional[dt.datetime] = None,
    inplace: bool,
    on_started: Optional[Callable[[Node], None]] = None,
    on_completed: Optional[Callable[[Node], None]] = None
) -> Tuple[Plan, Optional[Node]]:
    plan = get_mutable_plan(plan, inplace=inplace)
    stale_nodes = _get_stale_nodes(
        plan,
        registry,
        max_workers=max_workers,
        retry=retry,
        fresh_time=fresh_time,
        on_started=on_started,
        on_completed=on_completed,
    )
    read_node_lookup = {}
    required_nodes = set()
    for node, registry_value in registry.mapping.items():
        is_stale = node in stale_nodes
        write_node, read_node = _add_value_store(
            plan, node, registry_value, is_stale=is_stale
        )
        if write_node:
            required_nodes.add(write_node)
        read_node_lookup[node] = read_node
    if output_node:
        output_node = read_node_lookup.get(output_node, output_node)
    prune_plan(
        plan, required_nodes=required_nodes, output_node=output_node, inplace=True
    )
    return plan, output_node
