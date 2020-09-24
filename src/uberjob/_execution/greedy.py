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

from uberjob._util.networkx_util import topological_sort
from uberjob.graph import Dependency, Literal


def get_special_connected_components(graph):
    union_find = nx.utils.UnionFind(graph.nodes)
    for u, v, k in graph.edges(keys=True):
        if type(u) is not Literal and type(k) is not Dependency:
            union_find.union(u, v)
    return union_find.to_sets()


def condense_graph(graph, representative_to_component_mapping):
    node_to_representative_mapping = {
        node: representative
        for representative, nodes in representative_to_component_mapping.items()
        for node in nodes
    }
    g = nx.DiGraph()
    for node in graph.nodes:
        g.add_node(node_to_representative_mapping[node])
    for u, v in graph.edges():
        u = node_to_representative_mapping[u]
        v = node_to_representative_mapping[v]
        if u != v:
            g.add_edge(u, v)
    return g


def pred_search(graph, nodes):
    visited = set()
    nodes.reverse()
    while nodes:
        node = nodes.pop()
        if node in visited:
            continue
        yield node
        for predecessor in graph.pred[node]:
            nodes.append(predecessor)
        visited.add(node)


def get_components_graph_and_mapping(graph):
    """
    Return the contracted connected components graph and the mapping from nodes in the contracted graph to all of the
    nodes in the original graph that were contracted into them.
    """
    representative_to_component_mapping = {
        next(iter(component)): component
        for component in get_special_connected_components(graph)
    }
    components_graph = condense_graph(graph, representative_to_component_mapping)
    return components_graph, representative_to_component_mapping


def get_condensation_graph_and_mapping(graph):
    """
    Return the condensation graph and the mapping from nodes in the condensation graph to all of the nodes in the
    original graph that were contracted into them.
    """
    (
        components_graph,
        representative_to_component_mapping,
    ) = get_components_graph_and_mapping(graph)
    representative_to_strong_component_mapping = {
        next(iter(component)): [
            node
            for representative in component
            for node in representative_to_component_mapping[representative]
        ]
        for component in nx.algorithms.strongly_connected_components(components_graph)
    }
    condensation_graph = condense_graph(
        graph, representative_to_strong_component_mapping
    )
    return condensation_graph, representative_to_strong_component_mapping


def get_priority_mapping(graph):
    """
    Returns a mapping from node to priority, with the lowest priority being the most important.

    The approach is as follows:

    1.
    Connected components are computed for the entire graph, but non-argument edges and outgoing edges of Literals
    are ignored.

    2.
    A new directed graph is formed by contracting each connected component to a single node. The resulting graph is
    usually but not always a DAG. The condensation of this graph is then obtained. The condensation of a graph is the
    result of contracting each strongly-connected component to a single node, and it's always a DAG. It's important
    to note that each node in the condensation is the result of two rounds of node contraction.

    3.
    The nodes in the original graph are ordered by performing a topological sort of the condensation, and then mapping
    each node from the condensation back to the nodes that were contracted into it.

    4.
    The order of the pseudo-sinks is used to determine the priority ordering for all nodes.
    Each pseudo-sink and its ancestors are prioritized in order.
    Pseudo-sinks are nodes with no outgoing argument edges,
    """
    (
        condensation_graph,
        representative_to_component_mapping,
    ) = get_condensation_graph_and_mapping(graph)
    pseudo_sinks = [
        node
        for representative in topological_sort(condensation_graph)
        for node in representative_to_component_mapping[representative]
        if all(
            type(edge_key) is Dependency
            for u, v, edge_key in graph.out_edges(node, keys=True)
        )
    ]
    return {node: index for index, node in enumerate(pred_search(graph, pseudo_sinks))}
