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
import typing
from collections import OrderedDict

from uberjob._plan import Plan
from uberjob._registry import Registry
from uberjob._util import compact_repr, fully_qualified_name, validation
from uberjob.graph import (
    Call,
    Dependency,
    Graph,
    KeywordArg,
    Literal,
    Node,
    PositionalArg,
)

TEAL = (0, 154 / 255, 166 / 255)
ORANGE = (227 / 255, 114 / 255, 34 / 255)
PURPLE = (187 / 255, 47 / 255, 237 / 255)
GRAY = (0.4, 0.4, 0.4)


def default_style(registry: Registry = None):
    import nxv

    if registry is None:
        registry = Registry()

    style = nxv.Style(
        node=nxv.chain(
            [
                {
                    "shape": "box",
                    "style": "filled",
                    "margin": 0.05,
                    "width": 0,
                    "height": 0,
                    "fontcolor": "white",
                },
                nxv.switch(
                    lambda u, d: type(u),
                    {
                        Literal: {"fillcolor": TEAL},
                        Call: {"fillcolor": ORANGE},
                        Scope: {"fillcolor": GRAY},
                    },
                ),
                nxv.switch(
                    lambda u, d: u in registry, {True: {"fillcolor": PURPLE}, False: {}}
                ),
                nxv.switch(
                    lambda u, d: u in registry,
                    {
                        False: nxv.switch(
                            lambda u, d: type(u),
                            {
                                Literal: lambda u, d: {"label": compact_repr(u.value)},
                                Call: lambda u, d: {
                                    "label": fully_qualified_name(u.fn)
                                },
                                Scope: lambda u, d: {
                                    "label": "\n".join(
                                        [
                                            *(str(value) for value in d["scope"]),
                                            "{} {}".format(
                                                d["count"],
                                                "node" if d["count"] == 1 else "nodes",
                                            ),
                                        ]
                                    )
                                },
                            },
                        ),
                        True: nxv.switch(
                            lambda u, d: type(u),
                            {
                                Literal: lambda u, d: {
                                    "label": "\n".join(
                                        [compact_repr(u.value), repr(registry[u])]
                                    )
                                },
                                Call: lambda u, d: {
                                    "label": "\n".join(
                                        [fully_qualified_name(u.fn), repr(registry[u])]
                                    )
                                },
                            },
                        ),
                    },
                ),
            ]
        ),
        edge=nxv.chain(
            [
                {"arrowhead": "open", "arrowtail": "open"},
                nxv.switch(
                    lambda u, v, e, d: type(e),
                    {
                        Dependency: {"style": "dashed"},
                        PositionalArg: lambda u, v, e, d: {"label": e.index},
                        KeywordArg: lambda u, v, e, d: {"label": e.name},
                    },
                ),
            ]
        ),
    )

    style = nxv.compose([style, nxv.styles.font("Courier", 10)])

    return style


class Scope:
    pass


def render(
    plan: typing.Union[Plan, Graph, typing.Tuple[Plan, typing.Optional[Node]]],
    *,
    registry: Registry = None,
    predicate: typing.Callable[[Node, dict], bool] = None,
    level: typing.Optional[int] = None,
    format: typing.Optional[str] = None
) -> typing.Optional[bytes]:
    """
    Use :mod:`nxv` to render a plan's symbolic call graph.

    :param plan: A plan's symbolic call graph.
    :param registry: A registry to include in the visualization.
    :param predicate: An optional node predicate ``f(u, d)`` that determines whether a node ``u`` with
                      attribute dict ``d`` will be included in the render.
    :param level: Optional maximum number of scope levels to view. Nodes are grouped by scope[:level].
    :param format: The nxv/GraphViz output format to produce.
    :return: The rendered graph.
    """
    import nxv

    if (
        isinstance(plan, tuple)
        and len(plan) == 2
        and isinstance(plan[0], Plan)
        and (isinstance(plan[1], Node) or plan[1] is None)
    ):
        plan = plan[0]
    validation.assert_is_instance(plan, "plan", (Plan, Graph))
    graph = (plan.graph if isinstance(plan, Plan) else plan).copy()
    if predicate:
        graph.remove_nodes_from(
            [u for u, d in graph.nodes(data=True) if not predicate(u, d)]
        )

    if level is not None:
        scope_groups = OrderedDict()
        for u in graph.nodes():
            scope = u.scope
            if scope:
                scope_groups.setdefault(scope[:level], []).append(u)
        for scope, group in scope_groups.items():
            group = set(group)
            predecessors = set()
            successors = set()
            for w in group:
                predecessors.update(graph.predecessors(w))
                successors.update((v, e) for _, v, e in graph.out_edges(w, keys=True))
            scope_node = Scope()
            graph.add_node(scope_node, scope=scope, count=len(group))
            graph.remove_nodes_from(group)
            graph.add_edges_from(
                (predecessor, scope_node, Dependency())
                for predecessor in predecessors
                if predecessor not in group
            )
            graph.add_edges_from(
                (scope_node, successor, dependency)
                for successor, dependency in successors
                if successor not in group
            )

    style = default_style(registry)
    return nxv.render(
        graph,
        style,
        algorithm="dot",
        format=format,
    )
