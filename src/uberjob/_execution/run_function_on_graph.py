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
"""Functionality for running a function on a graph in parallel in topological order using a thread pool"""
import os
import threading
from contextlib import contextmanager
from typing import NamedTuple

from uberjob._errors import NodeError
from uberjob._execution.scheduler import create_queue
from uberjob._util.networkx_util import assert_acyclic, predecessor_count
from uberjob.graph import Node


def coerce_worker_count(worker_count):
    if worker_count is None:
        # Matches concurrent.futures.ThreadPoolExecutor in Python 3.8+.
        worker_count = min(32, (os.cpu_count() or 1) + 4)
    worker_count = int(worker_count)
    if worker_count < 1:
        raise ValueError("worker_count must be at least 1.")
    return worker_count


def coerce_max_errors(max_errors):
    if max_errors is None:
        return None
    max_errors = int(max_errors)
    if max_errors < 0:
        raise ValueError("max_errors must be nonnegative.")
    return max_errors


def thread(fn):
    t = threading.Thread(target=fn)
    t.start()
    return t


DONE = object()


def worker_thread(queue, process_item):
    def process_items():
        while True:
            item = queue.get()
            try:
                if item is DONE:
                    return
                process_item(item)
            finally:
                queue.task_done()

    return thread(process_items)


@contextmanager
def worker_pool(queue, process_item, worker_count):
    workers = []
    try:
        for _ in range(worker_count):
            workers.append(worker_thread(queue, process_item))
        yield
    finally:
        for worker in workers:
            worker.join()


class PreparedNodes(NamedTuple):
    source_nodes: list[Node]
    single_parent_nodes: set[Node]
    remaining_pred_count_mapping: dict[Node, int]


def prepare_nodes(graph) -> PreparedNodes:
    source_nodes = []
    single_parent_nodes = set()
    remaining_pred_count_mapping = {}
    for node in graph:
        count = predecessor_count(graph, node)
        if count == 0:
            source_nodes.append(node)
        elif count == 1:
            single_parent_nodes.add(node)
        else:
            remaining_pred_count_mapping[node] = count
    return PreparedNodes(
        source_nodes, single_parent_nodes, remaining_pred_count_mapping
    )


def coerce_node_error(node: Node, exception: Exception) -> NodeError:
    if isinstance(exception, NodeError):
        return exception
    node_error = NodeError(node)
    node_error.__cause__ = exception
    return node_error


def run_function_on_graph(
    graph, fn, *, worker_count=None, max_errors=0, scheduler=None
):
    assert_acyclic(graph)
    worker_count = coerce_worker_count(worker_count)
    max_errors = coerce_max_errors(max_errors)

    source_nodes, single_parent_nodes, remaining_pred_count_mapping = prepare_nodes(
        graph
    )
    remaining_pred_count_lock = threading.Lock()

    stop = False
    first_node_error = None
    error_count = 0
    failure_lock = threading.Lock()

    queue = create_queue(graph, source_nodes, scheduler)

    def process_node(node):
        nonlocal stop
        nonlocal first_node_error
        nonlocal error_count
        if stop:
            return
        try:
            fn(node)
        except BaseException as exception:
            with failure_lock:
                error_count += 1
                if not first_node_error:
                    first_node_error = coerce_node_error(node, exception)
                if max_errors is not None and error_count > max_errors:
                    stop = True
        else:
            for successor in graph.successors(node):
                if successor in single_parent_nodes:
                    queue.put(successor)
                else:
                    with remaining_pred_count_lock:
                        remaining_pred_count_mapping[successor] -= 1
                        if remaining_pred_count_mapping[successor] == 0:
                            queue.put(successor)

    with worker_pool(queue, process_node, worker_count):
        try:
            queue.join()
        finally:
            stop = True
            for _ in range(worker_count):
                queue.put(DONE)

    if first_node_error:
        raise first_node_error
