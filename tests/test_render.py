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
import uberjob
from uberjob._testing import TestStore


def add(x, y):
    return x + y


def test_render():
    plan = uberjob.Plan()
    plan.call(add, 2, 3)
    uberjob.render(plan, format="svg")


def test_render_registry():
    plan = uberjob.Plan()
    registry = uberjob.Registry()
    x = plan.call(add, 2, 3)
    registry.add(x, TestStore())
    uberjob.render(plan, registry=registry, format="svg")


def test_render_level():
    plan = uberjob.Plan()
    with plan.scope("x"):
        x = plan.call(add, 2, 3)
    plan.call(add, x, 4)
    uberjob.render(plan, level=1, format="svg")
