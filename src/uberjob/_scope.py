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
from uberjob.graph import Graph, Node, get_scope


def get_implicit_scope(graph: Graph, node: Node):
    return graph.nodes[node].get("implicit_scope", ())


def get_full_scope(graph: Graph, node: Node):
    return get_scope(graph, node) + get_implicit_scope(graph, node)


def set_implicit_scope(graph: Graph, node: Node, implicit_scope):
    if implicit_scope:
        graph.nodes[node]["implicit_scope"] = tuple(implicit_scope)
    elif "implicit_scope" in graph.nodes[node]:
        del graph.nodes[node]["implicit_scope"]


def set_full_scope(graph: Graph, node: Node, scope, implicit_scope):
    graph.nodes[node]["scope"] = tuple(scope)
    set_implicit_scope(graph, node, implicit_scope)
