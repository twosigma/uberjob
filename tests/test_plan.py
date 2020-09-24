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
import operator
import os
import pathlib
import re
import tempfile

import networkx as nx
from uberjob_testing.util import UberjobTestCase, copy_with_line_offset

import uberjob
from uberjob._util.traceback import get_stack_frame
from uberjob.graph import Call
from uberjob.progress import console_progress, default_progress, html_progress


class PlanTestCase(UberjobTestCase):
    def test_run_empty_plan(self):
        uberjob.run(uberjob.Plan())

    def test_failure_to_bind_raises_type_error_and_does_not_alter_plan(self):
        def f(a, b=0, *args, c, d=0, **kwargs):
            pass

        def g(a, b=0, *, c, d=0):
            pass

        p = uberjob.Plan()
        for func, args, kwargs in [
            (f, (), {}),
            (f, (0,), {}),
            (f, (), {"c": 0}),
            (g, (0, 0, 0), {}),
            (g, (0, 0), {"z": 0}),
        ]:
            with self.subTest(func=func, args=args, kwargs=kwargs):
                with self.assert_failed_to_bind():
                    p.call(func, *args, **kwargs)
        self.assertEqual(p.graph.number_of_nodes(), 0)

    def test_failed_run(self):
        def f(a, b):
            raise ValueError("Oops")

        p = uberjob.Plan()
        x = p.call(f, 2, 3)

        with self.assert_call_exception(ValueError, "Oops"):
            uberjob.run(p, output=x)

    def test_multiple_dependencies_on_single_node(self):
        p = uberjob.Plan()
        x = p.call(lambda value: value, 1)
        y = p.call(lambda *args, **kwargs: (args, kwargs), x, x, a=x, b=x)
        p.add_dependency(x, y)
        self.assertEqual(uberjob.run(p, output=y), ((1, 1), {"a": 1, "b": 1}))

    def test_structured_output(self):
        p = uberjob.Plan()
        x = p.call(lambda: 1)
        y = p.call(lambda n: n + 2, x)
        self.assertEqual(uberjob.run(p, output=[x, y]), [1, 3])
        self.assertEqual(uberjob.run(p, output=(x, y)), (1, 3))
        self.assertEqual(
            uberjob.run(p, output={"a": x, "b": [x, y], "c": (1, 2, 3, 4)}),
            {"a": 1, "b": [1, 3], "c": (1, 2, 3, 4)},
        )
        self.assertEqual(
            uberjob.run(p, output={(x, y): "a", (3, 4): "b"}),
            {(1, 3): "a", (3, 4): "b"},
        )
        self.assertEqual(uberjob.run(p, output={x, y}), {1, 3})
        self.assertEqual(uberjob.run(p, output=7), 7)
        self.assertEqual(uberjob.run(p, output=[]), [])
        self.assertEqual(uberjob.run(p, output=dict()), dict())

    def test_stack_frame_function(self):
        stack_frame1 = get_stack_frame(1)
        stack_frame2 = get_stack_frame(1)
        self.assertEqual(stack_frame1.path, __file__)
        self.assertEqual(stack_frame1.line + 1, stack_frame2.line)

    def test_stack_frame_basic(self):
        plan = uberjob.Plan()
        stack_frame = get_stack_frame(1)
        x = plan.call(operator.truediv, 1, 0)
        with self.assert_call_exception(
            expected_stack_frame=copy_with_line_offset(stack_frame, 1)
        ):
            uberjob.run(plan, output=x)

    def test_has_a_cycle(self):
        plan = uberjob.Plan()
        x = plan.call(operator.add, 1, 2)
        y = plan.call(operator.add, x, 4)
        plan.add_dependency(y, x)
        with self.assertRaises(nx.HasACycle):
            uberjob.run(plan, output=y)

    def test_plan_type_validation(self):
        plan = uberjob.Plan()
        with self.assertRaises(TypeError):
            plan.call(1)
        with self.assertRaises(TypeError):
            plan.lit(plan.lit(1))
        a = plan.call(operator.add, 1, 2)
        with self.assertRaises(TypeError):
            plan.add_dependency(a, 1)
        with self.assertRaises(TypeError):
            plan.add_dependency(1, a)

    def test_progress(self):
        plan = uberjob.Plan()
        x = plan.call(operator.add, 2, 2)
        y = plan.call(operator.add, x, 3)

        uberjob.run(plan, output=y, progress=None)
        uberjob.run(plan, output=y, progress=False)
        uberjob.run(plan, output=y, progress=True)
        uberjob.run(plan, output=y, progress=default_progress)
        uberjob.run(plan, output=y, progress=console_progress)
        uberjob.run(plan, output=y, progress=[])
        uberjob.run(plan, output=y, progress=[console_progress])
        uberjob.run(plan, output=y, progress=[console_progress, console_progress])
        with tempfile.TemporaryDirectory() as tempdir:
            for path in (
                os.path.join(tempdir, "progress.html"),
                pathlib.Path(tempdir) / "progress2.html",
            ):
                uberjob.run(plan, output=y, progress=html_progress(path))
        with self.assertRaises(TypeError):
            uberjob.run(plan, output=y, progress=7)

    def test_transform_physical(self):
        plan = uberjob.Plan()
        y = plan.call(operator.sub, 7, 4)
        x = plan.call(pow, 2, y)

        def replace_pow_with_add(p, output_node):
            nodes = [
                node
                for node in p.graph.nodes()
                if isinstance(node, Call) and node.fn == pow
            ]
            relabeling = {
                node: Call(operator.add, stack_frame=node.stack_frame) for node in nodes
            }
            nx.relabel.relabel_nodes(p.graph, relabeling, copy=False)
            return p, relabeling.get(output_node, output_node)

        self.assertEqual(
            uberjob.run(plan, output=x, transform_physical=replace_pow_with_add), 5
        )

    def test_scope_error_checking(self):
        plan = uberjob.Plan()
        with self.assertRaisesRegex(
            Exception,
            re.escape("Plan scopes must be entered and exited in stack order."),
        ):
            with plan.scope(1):
                s = plan.scope(2)
                s.__enter__()

    def test_add_dependency_error_checking(self):
        plan1 = uberjob.Plan()
        plan2 = uberjob.Plan()
        x = plan1.lit(1)
        y = plan2.lit(2)
        with self.assertRaisesRegex(Exception, re.escape("does not contain the node")):
            plan1.add_dependency(x, y)

    def test_schedulers(self):
        plan = uberjob.Plan()
        x = plan.call(operator.add, 2, 2)
        y = plan.call(operator.add, x, 3)
        for scheduler in [None, "default", "random"]:
            self.assertEqual(uberjob.run(plan, output=y, scheduler=scheduler), 7)
        with self.assertRaises(ValueError):
            uberjob.run(plan, output=y, scheduler="fizz")

    def test_max_errors(self):
        n = 10
        count_slot = [0]

        def fizz():
            count_slot[0] += 1
            raise ValueError()

        plan = uberjob.Plan()
        calls = [plan.call(fizz) for _ in range(n)]

        for max_errors in [None, *range(n)]:
            count_slot[0] = 0
            with self.subTest(max_errors=max_errors):
                with self.assertRaises(uberjob.CallError):
                    uberjob.run(
                        plan,
                        output=calls,
                        max_workers=1,
                        max_errors=max_errors,
                        progress=None,
                    )
                self.assertEqual(
                    count_slot[0], n if max_errors is None else max_errors + 1
                )

    def test_max_workers_and_max_errors_validation(self):
        plan = uberjob.Plan()
        with self.assertRaises(TypeError):
            uberjob.run(plan, max_workers="hello")
        with self.assertRaises(TypeError):
            uberjob.run(plan, max_workers=1.0)
        with self.assertRaises(TypeError):
            uberjob.run(plan, max_errors="hello")
        with self.assertRaises(TypeError):
            uberjob.run(plan, max_errors=1.0)
        with self.assertRaises(ValueError):
            uberjob.run(plan, max_workers=-1)
        with self.assertRaises(ValueError):
            uberjob.run(plan, max_workers=0)
        with self.assertRaises(ValueError):
            uberjob.run(plan, max_errors=-1)

    def test_retry_vanilla(self):
        count_slot = [0]

        def fizz():
            count_slot[0] += 1
            raise ValueError()

        plan = uberjob.Plan()
        call = plan.call(fizz)

        for retry in [None, 1, 2, 3]:
            count_slot[0] = 0
            with self.subTest(retry=retry):
                with self.assertRaises(uberjob.CallError):
                    uberjob.run(plan, output=call, retry=retry)
                self.assertEqual(count_slot[0], 1 if retry is None else retry)

    def test_retry_custom(self):
        retry_count_slot = [0]
        fizz_count_slot = [0]

        def create_retry(attempts):
            def inner_retry(f):
                def wrapper(*args, **kwargs):
                    for attempt_index in range(attempts):
                        retry_count_slot[0] += 1
                        try:
                            return f(*args, **kwargs)
                        except Exception:
                            is_last_attempt = attempt_index == attempts - 1
                            if is_last_attempt:
                                raise

                return wrapper

            return inner_retry

        def fizz(x):
            fizz_count_slot[0] += 1
            if fizz_count_slot[0] < 5:
                raise ValueError()
            return x * x

        plan = uberjob.Plan()
        y = plan.call(fizz, 3)

        with self.assertRaises(uberjob.CallError):
            uberjob.run(plan, output=y, retry=create_retry(2))
        self.assertEqual(retry_count_slot[0], 2)
        self.assertEqual(fizz_count_slot[0], 2)

        retry_count_slot = [0]
        fizz_count_slot = [0]
        self.assertEqual(uberjob.run(plan, output=y, retry=create_retry(999)), 9)
        self.assertEqual(retry_count_slot[0], 5)
        self.assertEqual(fizz_count_slot[0], 5)

    def test_retry_validation(self):
        plan = uberjob.Plan()
        with self.assertRaises(TypeError):
            uberjob.run(plan, retry="hello")
        with self.assertRaises(ValueError):
            uberjob.run(plan, retry=0)
        with self.assertRaises(ValueError):
            uberjob.run(plan, retry=-1)
