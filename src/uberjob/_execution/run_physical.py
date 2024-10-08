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
from collections.abc import Callable
from typing import Any, NamedTuple

from uberjob._errors import NodeError, create_chained_call_error
from uberjob._execution.run_function_on_graph import run_function_on_graph
from uberjob._graph import get_full_call_scope
from uberjob._plan import Plan
from uberjob._transformations.pruning import prune_source_literals
from uberjob._util import Slot
from uberjob._util.retry import identity
from uberjob.graph import Call, Graph, Literal, Node, get_argument_nodes
from uberjob.progress._null_progress_observer import NullProgressObserver
from uberjob.progress._progress_observer import ProgressObserver


class BoundCall:
    """A bound symbolic function call."""

    __slots__ = ("args", "kwargs", "result")

    def __init__(self, args, kwargs, result):
        self.args = args
        self.kwargs = kwargs
        self.result = result

    def run(self, fn, retry):
        args = [arg.value for arg in self.args]
        kwargs = {name: arg.value for name, arg in self.kwargs.items()}
        self.result.value = retry(fn)(*args, **kwargs)


def _create_bound_call(
    graph: Graph, call: Call, result_lookup: dict[Node, Any]
) -> BoundCall:
    args, kwargs = get_argument_nodes(graph, call)
    args = [result_lookup[predecessor] for predecessor in args]
    kwargs = {name: result_lookup[predecessor] for name, predecessor in kwargs.items()}
    result = result_lookup[call]
    return BoundCall(args, kwargs, result)


def _create_bound_call_lookup_and_output_slot(
    plan: Plan, output_node: Node | None = None
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
    bound_call_lookup: dict[Node, BoundCall]
    output_slot: Slot
    process: Callable[[Node], None]
    plan: Plan


def prep_run_physical(
    plan: Plan,
    *,
    inplace: bool,
    output_node: Node | None = None,
    retry: Callable[[Callable], Callable] | None = None,
    progress_observer: ProgressObserver | None = None,
):
    bound_call_lookup, output_slot = _create_bound_call_lookup_and_output_slot(
        plan, output_node
    )
    plan = prune_source_literals(plan, inplace=inplace)
    retry = retry or identity
    progress_observer = progress_observer or NullProgressObserver()

    def process(node):
        if type(node) is Call:
            scope = get_full_call_scope(node)
            progress_observer.increment_running(section="run", scope=scope)
            bound_call = bound_call_lookup[node]
            try:
                bound_call.value.run(node.fn, retry)
            except Exception as exception:
                # Drop internal frames
                exception.__traceback__ = exception.__traceback__.tb_next.tb_next
                progress_observer.increment_failed(
                    section="run",
                    scope=scope,
                    exception=create_chained_call_error(node, exception),
                )
                raise NodeError(node) from exception
            finally:
                bound_call.value = None
            progress_observer.increment_completed(section="run", scope=scope)

    return PrepRunPhysical(bound_call_lookup, output_slot, process, plan)


def run_physical(
    plan: Plan,
    *,
    inplace: bool,
    output_node: Node | None = None,
    retry: Callable[[Callable], Callable] | None = None,
    max_workers: int | None = None,
    max_errors: int | None = 0,
    scheduler: str | None = None,
    progress_observer: ProgressObserver,
) -> Any:
    _, output_slot, process, plan = prep_run_physical(
        plan,
        output_node=output_node,
        retry=retry,
        inplace=inplace,
        progress_observer=progress_observer,
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
