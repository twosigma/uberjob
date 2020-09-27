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
import os
import tempfile

import pytest

import uberjob


def add(x, y):
    return x + y


def fail():
    raise Exception("Something failed.")


def test_html_progress():
    plan = uberjob.Plan()
    x = plan.call(add, 2, 3)
    with tempfile.TemporaryDirectory() as temp:
        path = os.path.join(temp, "uberjob.html")
        uberjob.run(plan, output=x, progress=uberjob.progress.html_progress(path))


def test_html_progress_with_exception():
    plan = uberjob.Plan()
    x = plan.call(fail)
    with pytest.raises(uberjob.CallError):
        with tempfile.TemporaryDirectory() as temp:
            path = os.path.join(temp, "uberjob.html")
            uberjob.run(plan, output=x, progress=uberjob.progress.html_progress(path))


def test_ipython_progress():
    plan = uberjob.Plan()
    x = plan.call(add, 2, 3)
    uberjob.run(plan, output=x, progress=uberjob.progress.ipython_progress)


def test_ipython_progress_with_exception():
    plan = uberjob.Plan()
    x = plan.call(fail)
    with pytest.raises(uberjob.CallError):
        uberjob.run(plan, output=x, progress=uberjob.progress.ipython_progress)
