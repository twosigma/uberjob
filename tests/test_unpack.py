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
from unittest import TestCase

from uberjob_testing import util

import uberjob


def ones():
    while True:
        yield 1


class UnpackTestCase(TestCase):
    assert_call_exception = util.assert_call_exception

    def test_build_errors(self):
        plan = uberjob.Plan()
        x = plan.lit(range(7, 11))

        for length in [-1, None, 1.0]:
            with self.subTest(length=length):
                with self.assertRaises(ValueError):
                    plan.unpack(x, length)

        for length in [0, 1, 2, 3, 5, 6]:
            with self.subTest(length=length):
                with self.assertRaises(ValueError):
                    a, b, c, d = plan.unpack(x, length)

    def test_run_errors(self):
        for length in [0, 1, 2, 3, 5, 6]:
            for use_lit in [True, False]:
                with self.subTest(length=length, use_lit=use_lit):
                    plan = uberjob.Plan()
                    x = range(7, 7 + length)
                    if use_lit:
                        x = plan.lit(x)
                    a, b, c, d = plan.unpack(x, 4)
                    with self.assert_call_exception(expected_exception=ValueError):
                        uberjob.run(plan, output=(d, c, b, a))

    def test_run_error_infinite_iterable(self):
        plan = uberjob.Plan()
        a, b = plan.unpack(ones(), 2)
        with self.assert_call_exception(expected_exception=ValueError):
            uberjob.run(plan, output=(b, a))

    def test_basic(self):
        plan = uberjob.Plan()
        x = plan.lit(range(7, 11))
        a, b, c, d = plan.unpack(x, 4)
        self.assertEqual(uberjob.run(plan, output=(d, c, b, a)), (10, 9, 8, 7))

    def test_basic2(self):
        for length in [0, 1, 2, 3]:
            with self.subTest(length=length):
                plan = uberjob.Plan()
                t = tuple(range(7, 7 + length))
                self.assertEqual(uberjob.run(plan, output=plan.unpack(t, length)), t)
