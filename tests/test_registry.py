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
import datetime as dt
import itertools
import operator

import networkx as nx

import uberjob
from uberjob._testing import TestStore
from uberjob._util import Missing
from uberjob._util.traceback import get_stack_frame

from .util import UberjobTestCase, copy_with_line_offset


class RegistryTestCase(UberjobTestCase):
    def test_forgot_registry_with_source(self):
        p = uberjob.Plan()
        r = uberjob.Registry()
        x = r.source(p, TestStore(5))
        with self.assert_forgotten_registry():
            uberjob.run(p, output=x)

    def test_source_dependent_on_write(self):
        p = uberjob.Plan()
        r = uberjob.Registry()
        x = p.call(lambda: 5)
        r.add(x, TestStore())
        y = r.source(p, r[x])
        p.add_dependency(x, y)
        self.assertEqual(uberjob.run(p, output=y, registry=r), 5)

    def test_registry_simple(self):
        p = uberjob.Plan()
        r = uberjob.Registry()
        x = p.call(operator.add, 2, 3)
        s = TestStore()
        r.add(x, s)
        self.assertIn(x, r)
        self.assertSetEqual({x}, set(r))
        self.assertSetEqual({x}, set(r.keys()))
        self.assertSetEqual({s}, set(r.values()))
        self.assertSetEqual({(x, s)}, set(r.items()))
        uberjob.run(p, registry=r)
        self.assertEqual(r[x].read_count, 0)
        self.assertEqual(r[x].write_count, 1)
        uberjob.run(p, registry=r)
        self.assertEqual(r[x].read_count, 0)
        self.assertEqual(r[x].write_count, 1)
        uberjob.run(p, registry=r, stale_check_max_workers=1)

    def test_pruning(self):
        p = uberjob.Plan()
        r = uberjob.Registry()
        x = p.call(operator.floordiv, 1, 0)
        y = p.call(operator.floordiv, 14, 2)
        self.assertEqual(uberjob.run(p, output=y), 7)
        self.assertEqual(uberjob.run(p, output=y, registry=r), 7)
        with self.assert_call_exception(ZeroDivisionError):
            uberjob.run(p, output=x)
        with self.assert_call_exception(ZeroDivisionError):
            uberjob.run(p, output=x, registry=r)

    def test_dry_run(self):
        plan = uberjob.Plan()
        registry = uberjob.Registry()
        x = plan.lit(1)
        self.assertEqual(uberjob.run(plan, dry_run=True)[0].graph.number_of_nodes(), 0)
        store = TestStore()
        registry.add(x, store)
        physical_plan, _ = uberjob.run(plan, registry=registry, dry_run=True)
        self.assertEqual(plan.graph.number_of_nodes(), 1)
        self.assertGreater(physical_plan.graph.number_of_nodes(), 1)
        self.assertEqual(store.write_count, 0)
        uberjob.run(physical_plan, output=list(physical_plan.graph.nodes()))
        self.assertEqual(store.write_count, 1)

    def test_registry_can_lie(self):
        p = uberjob.Plan()
        r = uberjob.Registry()
        x = p.call(operator.add, 2, 3)
        r.add(x, TestStore(7))
        self.assertEqual(uberjob.run(p, output=x), 5)
        self.assertEqual(r[x].read_count, 0)
        self.assertEqual(r[x].write_count, 0)
        self.assertEqual(uberjob.run(p, output=x, registry=r), 7)

    def test_registry_complex(self):
        for x_stale, y_stale, z_stale, z_output in itertools.product(
            [False, True], repeat=4
        ):
            with self.subTest(
                x_stale=x_stale, y_stale=y_stale, z_stale=z_stale, z_output=z_output
            ):
                p = uberjob.Plan()
                r = uberjob.Registry()
                x = p.call(operator.add, 2, 3)
                y = p.call(operator.add, 4, 5)
                z = p.call(operator.add, x, y)
                r.add(x, TestStore(Missing if x_stale else uberjob.run(p, output=x)))
                r.add(y, TestStore(Missing if y_stale else uberjob.run(p, output=y)))
                r.add(z, TestStore(Missing if z_stale else uberjob.run(p, output=z)))
                self.assertEqual(
                    uberjob.run(p, output=z if z_output else None, registry=r),
                    14 if z_output else None,
                )
                self.assertEqual(r[x].read_count, int(x_stale or y_stale or z_stale))
                self.assertEqual(r[x].write_count, int(x_stale))
                self.assertEqual(r[y].read_count, int(x_stale or y_stale or z_stale))
                self.assertEqual(r[y].write_count, int(y_stale))
                self.assertEqual(r[z].read_count, int(z_output))
                self.assertEqual(r[z].write_count, int(x_stale or y_stale or z_stale))

    def test_fresh_time_basic(self):
        p = uberjob.Plan()
        r = uberjob.Registry()
        x = p.call(operator.add, 2, 3)
        r.add(x, TestStore())
        uberjob.run(p, registry=r)
        self.assertEqual(r[x].write_count, 1)
        uberjob.run(p, registry=r, fresh_time=r[x].modified_time)
        self.assertEqual(r[x].write_count, 1)
        uberjob.run(
            p, registry=r, fresh_time=r[x].modified_time + dt.timedelta(seconds=1)
        )
        self.assertEqual(r[x].write_count, 2)

    def test_fresh_time_advanced(self):
        p = uberjob.Plan()
        r = uberjob.Registry()
        t0 = dt.datetime.now()
        store_a = TestStore(7, modified_time=t0)
        a = r.source(p, store_a)
        store_c = TestStore()
        b = p.call(store_c.write, a)
        c = r.source(p, store_c)
        p.add_dependency(b, c)
        uberjob.run(p, registry=r)
        self.assertEqual(store_a.read_count, 1)
        self.assertEqual(store_c.read_count, 0)
        self.assertEqual(store_c.write_count, 1)
        self.assertGreaterEqual(store_c.modified_time, store_a.modified_time)
        uberjob.run(p, registry=r, fresh_time=store_c.modified_time)
        self.assertEqual(store_a.read_count, 1)
        self.assertEqual(store_c.read_count, 0)
        self.assertEqual(store_c.write_count, 1)
        uberjob.run(
            p, registry=r, fresh_time=store_c.modified_time + dt.timedelta(seconds=1)
        )
        self.assertEqual(store_a.read_count, 2)
        self.assertEqual(store_c.read_count, 0)
        self.assertEqual(store_c.write_count, 2)

    def test_failed_to_read_from_empty_store_1(self):
        p = uberjob.Plan()
        r = uberjob.Registry()
        x = r.source(p, TestStore())
        with self.assert_failed_to_read_from_empty_store():
            uberjob.run(p, registry=r, output=x)

    def test_failed_to_read_from_empty_store_2(self):
        p = uberjob.Plan()
        r = uberjob.Registry()
        x = r.source(p, TestStore())
        y = r.source(p, TestStore())
        p.add_dependency(x, y)
        with self.assert_failed_to_read_from_empty_store():
            uberjob.run(p, registry=r, output=y)

    def test_failed_to_get_modified_time(self):
        p = uberjob.Plan()
        r = uberjob.Registry()
        x = r.source(p, TestStore(can_get_modified_time=False))
        with self.assert_call_exception():
            uberjob.run(p, registry=r, output=x, dry_run=True)

    def test_dependent_source(self):
        p = uberjob.Plan()
        r = uberjob.Registry()

        x = r.source(p, TestStore(0))
        s = TestStore()
        y = p.call(s.write, x)
        z = r.source(p, s)
        p.add_dependency(y, z)

        self.assertEqual(uberjob.run(p, registry=r, output=z), 0)
        self.assertEqual(s.read_count, 1)
        self.assertEqual(s.write_count, 1)

        self.assertEqual(uberjob.run(p, registry=r, output=z), 0)
        self.assertEqual(s.read_count, 2)
        self.assertEqual(s.write_count, 1)

    def test_stale_source_successors_run_after_stale_source_predecessors(self):
        p = uberjob.Plan()
        r = uberjob.Registry()

        t0 = dt.datetime.utcnow()
        t1 = t0 + dt.timedelta(seconds=1)

        x = r.source(p, TestStore(1, modified_time=t1))
        s = TestStore(2, modified_time=t0)
        y = p.call(s.write, x)
        z = r.source(p, s)
        p.add_dependency(y, z)
        w = p.call(s.read)
        p.add_dependency(z, w)

        self.assertEqual(uberjob.run(p, registry=r, output=w), 1)

    def test_call_with_side_effects_but_no_args_or_return(self):
        p = uberjob.Plan()
        r = uberjob.Registry()

        xs = TestStore(0)
        x = r.source(p, xs)
        ys = TestStore()
        y = p.call(lambda: ys.write(xs.read()))
        z = r.source(p, ys)
        p.add_dependency(x, y)
        p.add_dependency(y, z)

        for i in range(2):
            with self.subTest(i=i):
                uberjob.run(p, registry=r)
                self.assertEqual(xs.read_count, 1)
                self.assertEqual(ys.read_count, 0)
                self.assertEqual(ys.write_count, 1)
                self.assertEqual(ys.value, 0)

    def test_complex_side_effects(self):
        m, n = 5, 3

        p = uberjob.Plan()
        r = uberjob.Registry()

        s = TestStore()
        x = [p.call(lambda k: k * k, i) for i in range(m)]
        w = [
            p.call(
                lambda k: s.write((s.read() if s.get_modified_time() else 0) + k), x[i]
            )
            for i in range(m)
        ]
        for a, b in (w[i : i + 2] for i in range(m - 1)):
            p.add_dependency(a, b)
        y = r.source(p, s)
        for i in range(m):
            p.add_dependency(w[i], y)
        z = [p.call(lambda k: k + s.read(), i) for i in range(n)]
        for i in range(n):
            p.add_dependency(y, z[i])

        for _ in range(2):
            self.assertEqual(
                uberjob.run(p, registry=r, output=z),
                [a + sum(b * b for b in range(m)) for a in range(n)],
            )

    def test_stack_frame_registry_add(self):
        plan = uberjob.Plan()
        registry = uberjob.Registry()
        x = plan.call(operator.add, 2, 2)
        stack_frame = get_stack_frame(1)
        registry.add(x, TestStore(can_write=False))
        with self.assert_call_exception(
            expected_stack_frame=copy_with_line_offset(stack_frame, 1)
        ):
            uberjob.run(plan, registry=registry, output=x)

    def test_stack_frame_registry_source(self):
        plan = uberjob.Plan()
        registry = uberjob.Registry()
        stack_frame = get_stack_frame(1)
        x = registry.source(plan, TestStore())
        with self.assert_call_exception(
            expected_stack_frame=copy_with_line_offset(stack_frame, 1)
        ):
            uberjob.run(plan, output=x)
        with self.assert_call_exception(
            expected_stack_frame=copy_with_line_offset(stack_frame, 1)
        ):
            uberjob.run(plan, registry=registry, output=x)

    def test_has_a_cycle(self):
        plan = uberjob.Plan()
        registry = uberjob.Registry()
        x = registry.source(plan, TestStore(1))
        y = registry.source(plan, TestStore(2))
        plan.add_dependency(x, y)
        plan.add_dependency(y, x)
        with self.assertRaises(nx.HasACycle):
            uberjob.run(plan, registry=registry, output=y)

    def test_registry_type_validation(self):
        plan = uberjob.Plan()
        registry = uberjob.Registry()
        a = plan.call(operator.add, 1, 2)
        with self.assertRaises(TypeError):
            registry.source(1, TestStore())
        with self.assertRaises(TypeError):
            registry.source(plan, 1)
        with self.assertRaises(TypeError):
            registry.add(1, TestStore())
        with self.assertRaises(TypeError):
            registry.add(a, 1)

    def test_registry_already_has_value_store(self):
        plan = uberjob.Plan()
        registry = uberjob.Registry()
        x = plan.call(operator.add, 1, 2)
        registry.add(x, TestStore())
        with self.assertRaisesRegex(Exception, r"The node already has a value store\."):
            registry.add(x, TestStore())

    def test_registry_copy(self):
        plan = uberjob.Plan()
        registry = uberjob.Registry()
        x = plan.call(operator.add, 2, 2)
        y = plan.call(operator.add, 2, 2)

        registry.add(x, TestStore())
        registry_copy = registry.copy()
        registry_copy.add(y, TestStore())
        registry_copy.mapping[x].is_source = True

        self.assertFalse(registry.mapping[x].is_source)
        self.assertNotIn(y, registry)
        self.assertIn(x, registry_copy)
        self.assertEqual(registry[x], registry_copy[x])

    def test_traceback_manipulation(self):
        def buzz():
            raise ValueError()

        def fizz():
            try:
                return buzz()
            except ValueError as e:
                raise Exception() from e

        class BadValueStore(uberjob.ValueStore):
            def read(self):
                raise NotImplementedError()

            def write(self):
                raise NotImplementedError()

            def get_modified_time(self):
                return fizz()

        class BadValueStore2(uberjob.ValueStore):
            def read(self):
                raise NotImplementedError()

            def write(self):
                raise NotImplementedError()

            def get_modified_time(self):
                return 7

        plan = uberjob.Plan()
        registry = uberjob.Registry()
        node = registry.source(plan, BadValueStore())

        with self.assert_call_exception(
            expected_exception_chain_traceback_summary=[
                ["get_modified_time", "fizz"],
                ["fizz", "buzz"],
            ]
        ):
            uberjob.run(plan, registry=registry, output=node)

        def bad_retry(f):
            raise Exception()

        with self.assert_call_exception(
            expected_exception_chain_traceback_summary=[["bad_retry"]]
        ):
            uberjob.run(plan, registry=registry, output=node, retry=bad_retry)

        plan = uberjob.Plan()
        registry = uberjob.Registry()
        node = registry.source(plan, BadValueStore2())

        with self.assert_call_exception(
            expected_exception_chain_traceback_summary=[["_to_naive_utc_time"]]
        ):
            uberjob.run(plan, registry=registry, output=node)
