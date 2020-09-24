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
import networkx as nx


def assert_acyclic(graph):
    for _ in topological_sort(graph):
        pass


def all_ancestors(graph, sources):
    """Returns all nodes having a path to any of the given sources."""
    visited = set()
    frontier = list(sources)
    while frontier:
        node = frontier.pop()
        if node in visited:
            continue
        visited.add(node)
        frontier.extend(graph.predecessors(node))
    return visited


def predecessor_count(graph, node) -> int:
    return len(graph.pred[node])


def source_nodes(graph):
    for node, predecessors in graph.pred.items():
        if not predecessors:
            yield node


def is_source_node(graph, node) -> bool:
    return not graph.pred[node]


def topological_sort(graph):
    """
    Yield the nodes in topological order.

    Faster than the networkx version. When fully iterated, raises an exception if the graph contains a cycle.
    """
    # Kahn's algorithm
    pred = graph.pred
    succ = graph.succ

    pred_count_mapping = {}
    q = []
    for node in graph.nodes:
        pred_count = len(pred[node])
        if pred_count:
            pred_count_mapping[node] = pred_count
        else:
            q.append(node)

    while q:
        node = q.pop()
        for successor in succ[node]:
            pred_count_mapping[successor] -= 1
            if pred_count_mapping[successor] == 0:
                q.append(successor)
        yield node

    if any(pred_count_mapping.values()):
        raise nx.HasACycle("The graph contains a cycle.")
