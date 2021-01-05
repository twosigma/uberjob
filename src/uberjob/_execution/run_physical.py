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
"""Functionality for executing a physical plan"""
from typing import Any, Callable, Dict, NamedTuple, Optional

from uberjob._execution.run_function_on_graph import run_function_on_graph
from uberjob._plan import Plan
from uberjob._transformations.pruning import prune_source_literals
from uberjob._util import Slot
from uberjob._util.retry import identity
from uberjob.graph import Call, Graph, Literal, Node, get_argument_nodes


class BoundCall:
    """A bound symbolic function call."""

    __slots__ = ("fn", "args", "kwargs", "result")

    def __init__(self, fn, args, kwargs, result):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.result = result

    def run(self):
        args = [arg.value for arg in self.args]
        kwargs = {name: arg.value for name, arg in self.kwargs.items()}
        self.result.value = self.fn(*args, **kwargs)


def _create_bound_call(
    graph: Graph, call: Call, result_lookup: Dict[Node, Any]
) -> BoundCall:
    args, kwargs = get_argument_nodes(graph, call)
    args = [result_lookup[predecessor] for predecessor in args]
    kwargs = {name: result_lookup[predecessor] for name, predecessor in kwargs.items()}
    result = result_lookup[call]
    return BoundCall(call.fn, args, kwargs, result)


def _create_bound_call_lookup_and_output_slot(
    plan: Plan, output_node: Optional[Node] = None
):
    result_lookup = {
        node: node if type(node) is Literal else Slot(None)
        for node in plan.graph.nodes()
    }
    bound_call_lookup = {
        node: Slot(_create_bound_call(plan.graph, node, result_lookup))
        for node in plan.graph.nodes()
        if type(node) is Call
    }
    output_slot = result_lookup[output_node] if output_node else None
    return bound_call_lookup, output_slot


class PrepRunPhysical(NamedTuple):
    bound_call_lookup: Dict[Node, BoundCall]
    output_slot: Slot
    process: Callable[[Node], None]
    plan: Plan


def _default_on_failed(node: Node, e: Exception):
    pass


def prep_run_physical(
    plan: Plan,
    *,
    inplace: bool,
    output_node: Optional[Node] = None,
    retry: Optional[Callable[[Callable], Callable]] = None,
    on_started: Optional[Callable[[Node], None]] = None,
    on_completed: Optional[Callable[[Node], None]] = None,
    on_failed: Optional[Callable[[Node, Exception], None]] = None,
):
    bound_call_lookup, output_slot = _create_bound_call_lookup_and_output_slot(
        plan, output_node
    )
    plan = prune_source_literals(plan, inplace=inplace)
    on_started = on_started or identity
    on_completed = on_completed or identity
    on_failed = on_failed or _default_on_failed
    retry = retry or identity

    def process(node):
        if type(node) is Call:
            on_started(node)
            bound_call = bound_call_lookup[node]
            try:
                retry(bound_call.value.run)()
            except Exception as e:
                on_failed(node, e)
                raise
            finally:
                bound_call.value = None

            on_completed(node)

    return PrepRunPhysical(bound_call_lookup, output_slot, process, plan)


def run_physical(
    plan: Plan,
    *,
    inplace: bool,
    output_node: Optional[Node] = None,
    retry: Optional[Callable[[Callable], Callable]] = None,
    max_workers: Optional[int] = None,
    max_errors: Optional[int] = 0,
    scheduler: Optional[str] = None,
    on_started: Optional[Callable[[Node], None]] = None,
    on_completed: Optional[Callable[[Node], None]] = None,
    on_failed: Optional[Callable[[Node, Exception], None]] = None,
) -> Any:
    _, output_slot, process, plan = prep_run_physical(
        plan,
        output_node=output_node,
        retry=retry,
        inplace=inplace,
        on_started=on_started,
        on_completed=on_completed,
        on_failed=on_failed,
    )

    run_function_on_graph(
        plan.graph,
        process,
        worker_count=max_workers,
        max_errors=max_errors,
        scheduler=scheduler,
    )

    return output_slot.value if output_slot else None


__all__ = ["prep_run_physical", "run_physical"]
