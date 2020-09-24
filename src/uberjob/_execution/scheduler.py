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
import random
from functools import total_ordering
from heapq import heappop, heappush
from queue import Queue

from uberjob._execution import greedy


class RandomQueue(Queue):
    def _init(self, maxsize):
        self.queue = []

    def _qsize(self):
        return len(self.queue)

    def _put(self, item):
        # Online Fisher-Yates shuffle
        self.queue.append(item)
        i = random.randrange(len(self.queue))
        self.queue[i], self.queue[-1] = self.queue[-1], self.queue[i]

    def _get(self):
        return self.queue.pop()


@total_ordering
class KeyValuePair:
    __slots__ = ("key", "value")

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __eq__(self, other):
        return self.key == other.key

    def __lt__(self, other):
        return self.key < other.key


class PriorityQueue(Queue):
    def __init__(self, priority, maxsize=0):
        super().__init__(maxsize)
        self.queue = []
        self.priority = priority

    def _qsize(self):
        return len(self.queue)

    def _put(self, item):
        heappush(self.queue, KeyValuePair(self.priority(item), item))

    def _get(self):
        return heappop(self.queue).value


def create_queue(graph, scheduler):
    scheduler = scheduler or "default"
    if scheduler == "cheap":
        return Queue()
    if scheduler == "random":
        return RandomQueue()
    if scheduler == "default":
        priority_mapping = greedy.get_priority_mapping(graph)
        # The priority mapping has priorities [0, n).
        # Setting the default priority to -1 gives the DONE sentinel highest priority.
        return PriorityQueue(lambda node: priority_mapping.get(node, -1))
    raise ValueError(f"Invalid scheduler {scheduler!r}")
