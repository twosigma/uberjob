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
from typing import Callable, Iterable, Optional, Tuple, Union

from uberjob._errors import CallError
from uberjob._execution.run_function_on_graph import NodeError
from uberjob._execution.run_physical import run_physical
from uberjob._plan import Plan
from uberjob._registry import Registry
from uberjob._transformations import get_mutable_plan
from uberjob._transformations.caching import plan_with_value_stores
from uberjob._transformations.pruning import prune_plan
from uberjob._util import fully_qualified_name
from uberjob._util.retry import create_retry
from uberjob._util.validation import assert_is_callable, assert_is_instance
from uberjob.graph import Call, Graph, Node
from uberjob.progress import (
    Progress,
    composite_progress,
    default_progress,
    null_progress,
)


def _get_full_scope(graph: Graph, node: Node):
    node_data = graph.nodes[node]
    return node_data["scope"] + node_data.get("implicit_scope", ())


def _get_scope_call_counts(plan: Plan):
    return collections.Counter(
        _get_full_scope(plan.graph, node)
        for node in plan.graph.nodes()
        if type(node) is Call
    )


def _prepare_plan_with_registry_and_progress(
    plan,
    registry,
    *,
    output_node,
    progress_observer,
    max_workers,
    retry,
    fresh_time,
    inplace,
):
    graph = plan.graph

    def get_stale_scope(node):
        scope = _get_full_scope(graph, node)
        value_store = registry.get(node)
        if not value_store:
            return scope
        return (*scope, fully_qualified_name(value_store.__class__))

    def on_started_stale_check(node):
        progress_observer.increment_running(
            section="stale", scope=get_stale_scope(node)
        )

    def on_completed_stale_check(node):
        progress_observer.increment_completed(
            section="stale", scope=get_stale_scope(node)
        )

    scope_counts = collections.Counter(
        get_stale_scope(node) for node in plan.graph.nodes() if type(node) is Call
    )
    for scope, count in scope_counts.items():
        progress_observer.increment_total(section="stale", scope=scope, amount=count)

    return plan_with_value_stores(
        plan,
        registry,
        output_node=output_node,
        max_workers=max_workers,
        retry=retry,
        fresh_time=fresh_time,
        inplace=inplace,
        on_started=on_started_stale_check,
        on_completed=on_completed_stale_check,
    )


def _run_physical_with_progress(
    plan,
    *,
    output_node,
    progress_observer,
    max_workers,
    max_errors,
    retry,
    scheduler,
    inplace,
):
    graph = plan.graph

    def on_started_run(node):
        progress_observer.increment_running(
            section="run", scope=_get_full_scope(graph, node)
        )

    def on_completed_run(node):
        progress_observer.increment_completed(
            section="run", scope=_get_full_scope(graph, node)
        )

    def on_failed_run(node, exception):
        call_error = CallError(node)
        call_error.__cause__ = exception
        progress_observer.increment_failed(
            section="run", scope=_get_full_scope(graph, node), exception=call_error
        )

    return run_physical(
        plan,
        output_node=output_node,
        max_workers=max_workers,
        max_errors=max_errors,
        retry=retry,
        scheduler=scheduler,
        inplace=inplace,
        on_started=on_started_run,
        on_completed=on_completed_run,
        on_failed=on_failed_run,
    )


def _coerce_progress(
    progress: Union[None, bool, Progress, Iterable[Progress]]
) -> Progress:
    if not progress:
        return null_progress
    try:
        progress = tuple(progress)
    except TypeError:
        pass
    if type(progress) is tuple:
        return composite_progress(*progress)
    if progress is True:
        return default_progress
    if not isinstance(progress, Progress):
        raise TypeError("The 'progress' parameter failed to coerce to a Progress.")
    return progress


def _coerce_retry(
    retry: Union[None, int, Callable[[Callable], Callable]]
) -> Callable[[Callable], Callable]:
    if callable(retry):
        return retry
    return create_retry(1 if retry is None else retry)


def run(
    plan: Plan,
    *,
    output=None,
    registry: Optional[Registry] = None,
    dry_run: bool = False,
    max_workers: Optional[int] = None,
    max_errors: Optional[int] = 0,
    retry: Union[None, int, Callable[[Callable], Callable]] = None,
    fresh_time: Optional[dt.datetime] = None,
    progress: Union[None, bool, Progress, Iterable[Progress]] = True,
    scheduler: Optional[str] = None,
    transform_physical: Optional[Callable[[Plan, Node], Tuple[Plan, Node]]] = None,
    stale_check_max_workers: Optional[int] = None,
):
    """
    Run a :class:`~uberjob.Plan`.

    - Returns any requested outputs.
    - Ensures that every :class:`~uberjob.ValueStore` in the :class:`~uberjob.Registry` is up to date.
    - When an error occurs, waits until there are no calls in flight and either no calls remain or the error limit
      has been exceeded. Once this condition has been met, the first error is raised.

    :param plan: The :class:`~uberjob.Plan` to run.
    :param output: An optional symbolic output specification. It will be converted via :meth:`~uberjob.Plan.gather`.
    :param registry: An optional :class:`~uberjob.Registry` that may specify a
                     :class:`~uberjob.ValueStore` for each :class:`~uberjob.graph.Node`.
    :param dry_run: If True, returns the physical plan and output node without actually running it.
    :param max_workers: The maximum number of threads that can be used to run the :class:`~uberjob.Plan`.
                        The default behavior is based on the core count.
    :param max_errors: The maximum number of errors that can occur before new calls stop being executed.
                       Specifying ``None`` will run as much of the graph as possible.
    :param retry: Optionally specify how many times to attempt each call, or pass a retry decorator.
                  The default behavior is to attempt each call only once.
    :param fresh_time: Stored values older than this will not be used.
    :param progress: Optionally specify how to observe progress.
    :param scheduler: Optionally specify the scheduler, which is used to choose what node to process next.
                      ``'default'`` and ``'random'`` are the available schedulers,
                      ``'default'`` attempts to finish parts of the plan before starting others.
                      ``'random'`` chooses a random node.
    :param transform_physical: Optional transformation to be applied to the physical plan.
                               It takes ``(plan, output_node)`` as positional arguments and
                               returns ``(transformed_plan, redirected_output_node)``.
    :param stale_check_max_workers: Optionally specify the maximum number of threads used for the stale check.
                                    The default behavior is to use ``max_workers``.
    :return: The non-symbolic output corresponding to the symbolic output argument.
    """
    assert_is_instance(plan, "plan", Plan)
    assert_is_instance(registry, "registry", Registry, optional=True)
    assert_is_instance(dry_run, "dry_run", bool)
    assert_is_instance(fresh_time, "fresh_time", dt.datetime, optional=True)
    assert_is_callable(transform_physical, "transform_physical", optional=True)
    assert_is_instance(scheduler, "scheduler", str, optional=True)
    if scheduler is not None and scheduler not in ("default", "random"):
        raise ValueError(f"Invalid scheduler {scheduler!r}")
    assert_is_instance(max_workers, "max_workers", int, optional=True)
    if max_workers is not None and max_workers < 1:
        raise ValueError("max_workers must be at least 1.")
    assert_is_instance(
        stale_check_max_workers, "stale_check_max_workers", int, optional=True
    )
    if stale_check_max_workers is not None and stale_check_max_workers < 1:
        raise ValueError("stale_check_max_workers must be at least 1.")
    assert_is_instance(max_errors, "max_errors", int, optional=True)
    if max_errors is not None and max_errors < 0:
        raise ValueError("max_errors must be nonnegative.")
    if stale_check_max_workers is None:
        stale_check_max_workers = max_workers

    plan = get_mutable_plan(plan, inplace=False)

    output_node = plan.gather(output) if output is not None else None
    redirected_output_node = output_node

    progress = _coerce_progress(progress)
    progress_observer = progress.observer()

    retry = _coerce_retry(retry)

    try:
        with progress_observer:
            if registry:
                plan, redirected_output_node = _prepare_plan_with_registry_and_progress(
                    plan,
                    registry,
                    output_node=output_node,
                    progress_observer=progress_observer,
                    max_workers=stale_check_max_workers,
                    retry=retry,
                    fresh_time=fresh_time,
                    inplace=True,
                )
            else:
                prune_plan(
                    plan, required_nodes=[], output_node=output_node, inplace=True
                )

            if transform_physical:
                plan, redirected_output_node = transform_physical(
                    plan, redirected_output_node
                )

            for scope, count in _get_scope_call_counts(plan).items():
                progress_observer.increment_total(
                    section="run", scope=scope, amount=count
                )

            if dry_run:
                return plan, redirected_output_node

            return _run_physical_with_progress(
                plan,
                output_node=redirected_output_node,
                progress_observer=progress_observer,
                max_workers=max_workers,
                max_errors=max_errors,
                retry=retry,
                scheduler=scheduler,
                inplace=True,
            )
    except NodeError as e:
        raise CallError(e.node) from e.__cause__
