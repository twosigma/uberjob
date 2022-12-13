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
import collections
import datetime as dt
from typing import Optional, Set, Tuple

from uberjob._errors import create_chained_call_error
from uberjob._execution.run_function_on_graph import run_function_on_graph
from uberjob._graph import get_full_call_scope
from uberjob._plan import Plan
from uberjob._registry import Registry, RegistryValue
from uberjob._transformations import get_mutable_plan
from uberjob._transformations.pruning import prune_plan, prune_source_literals
from uberjob._util import Slot, fully_qualified_name, safe_max
from uberjob.graph import Call, Dependency, KeywordArg, Node, PositionalArg, get_scope
from uberjob.progress._progress_observer import ProgressObserver


class BarrierType:
    __slots__ = ()

    def __repr__(self):
        return "Barrier"


Barrier = BarrierType()


def _to_naive_utc_time(value: Optional[dt.datetime]) -> Optional[dt.datetime]:
    return (
        value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        if value and value.tzinfo
        else value
    )


def _get_stale_scope(call: Call, registry: Registry) -> Tuple:
    scope = get_full_call_scope(call)
    value_store = registry.get(call)
    if value_store is None:
        return scope
    return (*scope, fully_qualified_name(value_store.__class__))


def _get_stale_nodes(
    plan: Plan,
    registry: Registry,
    *,
    retry,
    max_workers: Optional[int] = None,
    fresh_time: Optional[dt.datetime] = None,
    progress_observer: ProgressObserver,
) -> Set[Node]:
    plan = prune_source_literals(
        plan, inplace=False, predicate=lambda node: node not in registry
    )
    fresh_time = _to_naive_utc_time(fresh_time)
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
        modified_time = _to_naive_utc_time(retry(value_store.get_modified_time)())
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
        if type(node) is Call:
            scope = _get_stale_scope(node, registry)
            progress_observer.increment_running(section="stale", scope=scope)
            try:
                process(node)
            except Exception as exception:
                progress_observer.increment_failed(
                    section="stale",
                    scope=scope,
                    exception=create_chained_call_error(node, exception),
                )
                raise
            progress_observer.increment_completed(section="stale", scope=scope)
        else:
            process(node)

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


def _update_stale_totals(
    plan: Plan, registry: Registry, progress_observer: ProgressObserver
) -> None:
    scope_counts = collections.Counter(
        _get_stale_scope(node, registry)
        for node in plan.graph.nodes()
        if type(node) is Call
    )
    for scope, count in scope_counts.items():
        progress_observer.increment_total(section="stale", scope=scope, amount=count)


def plan_with_value_stores(
    plan: Plan,
    registry: Registry,
    *,
    output_node: Optional[Node],
    max_workers: Optional[int] = None,
    retry,
    fresh_time: Optional[dt.datetime] = None,
    inplace: bool,
    progress_observer,
) -> Tuple[Plan, Optional[Node]]:
    _update_stale_totals(plan, registry, progress_observer)
    plan = get_mutable_plan(plan, inplace=inplace)
    stale_nodes = _get_stale_nodes(
        plan,
        registry,
        max_workers=max_workers,
        retry=retry,
        fresh_time=fresh_time,
        progress_observer=progress_observer,
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
