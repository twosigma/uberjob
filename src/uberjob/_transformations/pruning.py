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
import itertools
from collections.abc import Callable, Iterable

from uberjob._plan import Plan
from uberjob._transformations import get_mutable_plan
from uberjob._util.networkx_util import all_ancestors, is_source_node
from uberjob.graph import Dependency, Literal, Node


def prune_plan(
    plan: Plan,
    *,
    required_nodes: Iterable[Node],
    output_node: Node | None,
    inplace: bool
) -> Plan:
    required_nodes = set(required_nodes)
    if output_node:
        required_nodes.add(output_node)
    plan = get_mutable_plan(plan, inplace=inplace)
    required_nodes = all_ancestors(plan.graph, required_nodes)
    prune_nodes = set(plan.graph.nodes()) - required_nodes
    plan.graph.remove_nodes_from(prune_nodes)

    for literal in [
        u for u in plan.graph.nodes() if type(u) is Literal and u != output_node
    ]:
        _prune_literal_if_trivial(plan, literal)

    return plan


def _prune_literal_if_trivial(plan: Plan, literal: Literal) -> None:
    """
    Prunes the literal if:
    a) it is not an argument to any function, and
    b) removing it does not increase the number of dependencies in the graph.
    """
    if not all(
        type(dependency) is Dependency
        for _, _, dependency in plan.graph.out_edges(literal, keys=True)
    ):
        return

    predecessors = list(plan.graph.predecessors(literal))
    successors = list(plan.graph.successors(literal))

    m = len(predecessors)
    n = len(successors)

    if m * n > m + n:
        return

    for predecessor, successor in itertools.product(predecessors, successors):
        plan.graph.add_edge(predecessor, successor, Dependency())
    plan.graph.remove_node(literal)


def prune_source_literals(
    plan: Plan, *, inplace: bool, predicate: Callable[[Literal], bool] | None = None
) -> Plan:
    """
    Prunes source literals. When predicate is present, the literal will only be pruned if it returns true.
    """
    plan = get_mutable_plan(plan, inplace=inplace)
    graph = plan.graph
    source_literals = [
        node for node in graph if type(node) is Literal and is_source_node(graph, node)
    ]
    if predicate:
        source_literals = [node for node in source_literals if predicate(node)]
    for node in source_literals:
        graph.remove_node(node)
    return plan
