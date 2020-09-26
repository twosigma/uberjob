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
"""
The basic strategy in this file is that the "bound call lookup" is the thing that stores all of the values that travel
along edges in the graph. Here plans operate on special "BigData" objects. After every function call, all of the
BigData objects that are in the bound call lookup are counted, and a running max is kept. After a plan has been
fully executed, it is asserted that the max is no greater than some threshold.
"""
import random
from unittest import TestCase

import uberjob
from uberjob._execution.run_function_on_graph import run_function_on_graph
from uberjob._execution.run_physical import prep_run_physical
from uberjob._testing import TestStore


class BigData:
    pass


def optional(value):
    return (value,) if value else ()


def count_big_datas(bound_call_lookup):
    return len(
        {
            slot.value
            for bound_call_slot in bound_call_lookup.values()
            for bound_call in optional(bound_call_slot.value)
            for slot in (
                *bound_call.args,
                *bound_call.kwargs.values(),
                bound_call.result,
            )
            if type(slot.value) is BigData
        }
    )


def create_chain(plan, registry, fn):
    x = registry.source(plan, TestStore(BigData()))
    y = plan.call(fn, x)
    registry.add(y, TestStore())
    return y


def create_join(plan, registry, fn):
    x1 = registry.source(plan, TestStore(BigData()))
    x2 = registry.source(plan, TestStore(BigData()))
    y = plan.call(fn, x1, x2)
    registry.add(y, TestStore())
    return y


def create_fork(plan, registry, fn):
    x = registry.source(plan, TestStore(BigData()))
    y1 = plan.call(fn, x)
    y2 = plan.call(fn, x)
    registry.add(y1, TestStore())
    registry.add(y2, TestStore())
    return y1, y2


def create_criss_cross1(plan, registry, fn):
    x1 = registry.source(plan, TestStore(BigData()))
    x2 = registry.source(plan, TestStore(BigData()))
    y1 = plan.call(fn, x1, x2)
    y2 = plan.call(fn, x1, x2)
    registry.add(y1, TestStore())
    registry.add(y2, TestStore())
    return y1, y2


def create_criss_cross2(plan, registry, fn):
    x1 = registry.source(plan, TestStore(BigData()))
    x2 = registry.source(plan, TestStore(BigData()))
    y1 = plan.call(fn, x1)
    y2 = plan.call(fn, x2)
    z1 = plan.call(fn, y1)
    z2 = plan.call(fn, y2)
    plan.add_dependency(y1, z2)
    plan.add_dependency(y2, z1)
    registry.add(z1, TestStore())
    registry.add(z2, TestStore())
    return z1, z2


def create_many_to_one_join(plan, registry, fn):
    xs = [registry.source(plan, TestStore(BigData())) for _ in range(8)]
    y = registry.source(plan, TestStore(BigData()))
    zs = []
    for x in xs:
        z = plan.call(fn, x, y)
        registry.add(z, TestStore())
        zs.append(z)
    return zs


def create_zipper(plan, registry, fn, length):
    x = registry.source(plan, TestStore(BigData()))
    for _ in range(length):
        y = registry.source(plan, TestStore(BigData()))
        x = plan.call(fn, x, y)
        registry.add(x, TestStore())
    return x


class BoundCallLookupInspector:
    def __init__(self):
        self.max_count = 0
        self.bound_call_lookup = None

    def foo(self, *args):
        self.max_count = max(self.max_count, count_big_datas(self.bound_call_lookup))
        return args[0]


def shuffle_plan(plan):
    edges = list(plan.graph.edges(keys=True))
    nodes_with_attrs = list(plan.graph.nodes(data=True))
    random.shuffle(nodes_with_attrs)
    random.shuffle(edges)
    new_plan = uberjob.Plan()
    for node, attrs in nodes_with_attrs:
        new_plan.graph.add_node(node, **attrs)
    for u, v, k in edges:
        new_plan.graph.add_edge(u, v, k)
    return new_plan


def shuffle_registry(registry):
    pairs = list(registry.mapping.items())
    random.shuffle(pairs)
    new_registry = uberjob.Registry()
    new_registry.mapping = dict(pairs)
    return new_registry


class SchedulerTestCase(TestCase):
    def assert_max_big_datas(
        self, plan, registry, inspector, max_allowed_count, scheduler=None
    ):
        plan = shuffle_plan(plan)
        registry = shuffle_registry(registry)
        physical_plan = uberjob.run(plan, registry=registry, dry_run=True)
        bound_call_lookup, output_slot, process, physical_plan = prep_run_physical(
            physical_plan, inplace=True
        )
        inspector.bound_call_lookup = bound_call_lookup
        run_function_on_graph(
            physical_plan.graph, process, worker_count=1, scheduler=scheduler
        )
        self.assertLessEqual(inspector.max_count, max_allowed_count)

    def _test_repeated_structure(
        self, build_structure, structure_count, max_allowed_count, scheduler=None
    ):
        plan = uberjob.Plan()
        registry = uberjob.Registry()
        inspector = BoundCallLookupInspector()
        for _ in range(structure_count):
            build_structure(plan, registry, inspector.foo)
        self.assert_max_big_datas(
            plan, registry, inspector, max_allowed_count, scheduler
        )

    def test_chain(self):
        self._test_repeated_structure(create_chain, 16, 1)

    def test_fork(self):
        self._test_repeated_structure(create_fork, 16, 1)

    def test_join(self):
        self._test_repeated_structure(create_join, 16, 2)

    def test_many_to_one_join(self):
        self._test_repeated_structure(create_many_to_one_join, 4, 2)

    def test_criss_cross1(self):
        self._test_repeated_structure(create_criss_cross1, 16, 2)

    def test_criss_cross2(self):
        self._test_repeated_structure(create_criss_cross2, 16, 2)

    # TODO: Figure out why this test sometimes fails on Windows
    # def test_zipper(self):
    #     plan = uberjob.Plan()
    #     registry = uberjob.Registry()
    #     inspector = BoundCallLookupInspector()
    #     reduce(
    #         lambda a, b: plan.call(inspector.foo, a, b),
    #         (create_zipper(plan, registry, inspector.foo, 8) for _ in range(8)),
    #     )
    #     self.assert_max_big_datas(plan, registry, inspector, 2)
