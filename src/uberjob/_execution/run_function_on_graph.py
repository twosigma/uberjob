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

from uberjob._execution.scheduler import create_queue
from uberjob._util import Slot
from uberjob._util.networkx_util import assert_acyclic, predecessor_count, source_nodes


class NodeError(Exception):
    """An exception was raised during _execution of a node."""

    def __init__(self, node):
        super().__init__(
            f"An exception was raised during _execution of the following node: {node!r}."
        )
        self.node = node


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
        yield queue.put
    finally:
        for worker in workers:
            worker.join()


class LockDict:
    def __init__(self, lock_count):
        self.locks = [threading.Lock() for _ in range(lock_count)]

    def __getitem__(self, key):
        return self.locks[hash(key) % len(self.locks)]


def run_function_on_graph(
    graph, fn, *, worker_count=None, max_errors=0, scheduler=None
):
    assert_acyclic(graph)
    worker_count = coerce_worker_count(worker_count)
    max_errors = coerce_max_errors(max_errors)
    failure_lock = threading.Lock()
    node_locks = LockDict(8)
    remaining_predecessor_counts = {
        node: Slot(predecessor_count(graph, node)) for node in graph
    }
    stop = False
    first_node_error = None
    error_count = 0

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
                    first_node_error = NodeError(node)
                    first_node_error.__cause__ = exception
                if max_errors is not None and error_count > max_errors:
                    stop = True
        else:
            for successor in graph.successors(node):
                remaining_predecessor_count = remaining_predecessor_counts[successor]
                with node_locks[successor]:
                    remaining_predecessor_count.value -= 1
                    should_submit = remaining_predecessor_count.value == 0
                if should_submit:
                    submit(successor)

    queue = create_queue(graph, scheduler)
    with worker_pool(queue, process_node, worker_count) as submit:
        try:
            for node in source_nodes(graph):
                submit(node)
            queue.join()
        finally:
            stop = True
            for _ in range(worker_count):
                queue.put(DONE)

    if first_node_error:
        raise first_node_error
