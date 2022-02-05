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
import re

import uberjob

EXPECTED_VERSION = "1.0.0"
REPOSITORY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def get_version_from_package():
    return uberjob.__version__


def get_version_from_pyproject_toml():
    path = os.path.join(REPOSITORY_ROOT, "pyproject.toml")
    with open(path) as f:
        text = f.read()
    return re.search(r"^\s*version\s*=\s*\"([^\s]+)\"\s*$", text, re.MULTILINE).group(1)


def test_version():
    assert get_version_from_package() == EXPECTED_VERSION
    assert get_version_from_pyproject_toml() == EXPECTED_VERSION
