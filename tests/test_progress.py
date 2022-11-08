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
import os
import tempfile
from unittest import TestCase

import uberjob
from uberjob.progress._simple_progress_observer import get_elapsed_string


def add(x, y):
    return x + y


def fail():
    raise Exception("Something failed.")


class ProgressTestCase(TestCase):
    def test_html_progress(self):
        plan = uberjob.Plan()
        x = plan.call(add, 2, 3)
        with tempfile.TemporaryDirectory() as temp:
            path = os.path.join(temp, "uberjob.html")
            uberjob.run(plan, output=x, progress=uberjob.progress.html_progress(path))

    def test_html_progress_with_exception(self):
        plan = uberjob.Plan()
        x = plan.call(fail)
        with self.assertRaises(uberjob.CallError):
            with tempfile.TemporaryDirectory() as temp:
                path = os.path.join(temp, "uberjob.html")
                uberjob.run(
                    plan, output=x, progress=uberjob.progress.html_progress(path)
                )

    def test_ipython_progress(self):
        plan = uberjob.Plan()
        x = plan.call(add, 2, 3)
        uberjob.run(plan, output=x, progress=uberjob.progress.ipython_progress)

    def test_ipython_progress_with_exception(self):
        plan = uberjob.Plan()
        x = plan.call(fail)
        with self.assertRaises(uberjob.CallError):
            uberjob.run(plan, output=x, progress=uberjob.progress.ipython_progress)

    def test_get_elapsed_string(self):
        for t, s in [
            (dt.timedelta(seconds=0), "0s"),
            (dt.timedelta(microseconds=999999), "0s"),
            (dt.timedelta(seconds=1.23), "1s"),
            (dt.timedelta(minutes=1, seconds=2), "1m02s"),
            (dt.timedelta(hours=1, minutes=2, seconds=3), "1h02m03s"),
            (dt.timedelta(hours=999, minutes=2, seconds=34), "999h02m34s"),
        ]:
            self.assertEqual(get_elapsed_string(t.total_seconds()), s)
