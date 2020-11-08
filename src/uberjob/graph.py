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
"""Provides the underlying graph, node, and edge classes used by the :class:`~uberjob.Plan`."""
from typing import Callable, Dict, List, Tuple

import networkx as nx

from uberjob._util import Omitted, repr_helper

Graph = nx.MultiDiGraph
"""A symbolic call graph."""


class Node:
    """A symbolic value in a call :class:`~uberjob.graph.Graph`."""

    __slots__ = ()

    def __repr__(self):
        return repr_helper(self)


class Literal(Node):
    """
    A symbolic literal value.

    :param value: The value of the literal.
    """

    __slots__ = ("value",)

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return repr_helper(self, self.value)


class Call(Node):
    """
    A symbolic function call.

    :param fn: The callable function.
    :param stack_frame: The stack frame of the call site.
    """

    __slots__ = ("fn", "stack_frame")

    def __init__(self, fn: Callable, *, stack_frame=None):
        self.fn = fn
        self.stack_frame = stack_frame

    def __repr__(self):
        return repr_helper(
            self,
            self.fn,
            stack_frame=Omitted if self.stack_frame else None,
            defaults={"stack_frame": None},
        )


class Dependency:
    """A dependency in a call :class:`~uberjob.graph.Graph` that links a :class:`~uberjob.graph.Node` to
    a :class:`~uberjob.graph.Node` that must run after it."""

    __slots__ = ()

    def __repr__(self):
        return repr_helper(self)

    def __hash__(self):
        return 0

    def __eq__(self, other):
        return type(other) is Dependency


class PositionalArg(Dependency):
    """
    A dependency in a call :class:`~uberjob.graph.Graph` that links a symbolic positional
    argument :class:`~uberjob.graph.Node` to a symbolic function :class:`~uberjob.graph.Call`.

    :param index: The index of the argument.
    """

    __slots__ = ("index",)

    def __init__(self, index: int):
        self.index = index

    def __repr__(self):
        return repr_helper(self, self.index)

    def __hash__(self):
        return hash(self.index)

    def __eq__(self, other):
        return type(other) is PositionalArg and self.index == other.index


class KeywordArg(Dependency):
    """
    A dependency in a call :class:`~uberjob.graph.Graph` that links a symbolic keyword
    argument :class:`~uberjob.graph.Node` to a symbolic function :class:`~uberjob.graph.Call`.

    :param name: The parameter name.
    :param index: The index of the argument; required because keyword arguments are ordered in Python 3.6+.
    """

    __slots__ = ("name", "index")

    def __init__(self, name: str, index: int):
        self.name = name
        self.index = index

    def __repr__(self):
        return repr_helper(self, self.name, self.index)

    def __hash__(self):
        return hash((self.name, self.index))

    def __eq__(self, other):
        return (
            type(other) is KeywordArg
            and self.index == other.index
            and self.name == other.name
        )


def get_argument_nodes(graph: Graph, call: Call) -> Tuple[List[Node], Dict[str, Node]]:
    """
    Return the symbolic args and kwargs of the given :class:`~uberjob.graph.Call`.

    :param graph: The graph.
    :param call: The call.
    """
    in_edges = graph.in_edges(call, keys=True)

    args = []
    keyword_arg_pairs = []
    for _, _, edge_key in in_edges:
        if type(edge_key) is PositionalArg:
            args.append(None)
        elif type(edge_key) is KeywordArg:
            keyword_arg_pairs.append(None)

    for predecessor, _, edge_key in in_edges:
        if type(edge_key) is PositionalArg:
            args[edge_key.index] = predecessor
        elif type(edge_key) is KeywordArg:
            keyword_arg_pairs[edge_key.index] = edge_key.name, predecessor

    return args, dict(keyword_arg_pairs)


def get_scope(graph: Graph, node: Node) -> Tuple:
    """
    Return the scope of the given :class:`~uberjob.graph.Node`.

    :param graph: The graph.
    :param node: The node.
    """
    return graph.nodes[node]["scope"]


__all__ = [
    "Graph",
    "Node",
    "Call",
    "Literal",
    "Dependency",
    "PositionalArg",
    "KeywordArg",
    "get_argument_nodes",
    "get_scope",
]
