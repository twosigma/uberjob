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
**uberjob** is a Python package for building and running call graphs.

Its powerful primitives can be combined to build massive and sophisticated pipelines. Previous outputs are automatically
reused when possible, providing incremental builds for free and enabling seamless recovery from errors.
This eliminates the need for a heavyweight scheduling platform, and makes development and production environments
simpler and more consistent.

The package is built around two core classes: :class:`uberjob.Plan` and :class:`uberjob.Registry`.
A Plan represents a call graph and a Registry maintains how and where
values are stored.
"""

__author__ = "Daniel Shields, Timothy Shields"
__maintainer__ = "Daniel Shields, Timothy Shields"
__email__ = "Daniel.Shields@twosigma.com, Timothy.Shields@twosigma.com"
__version__ = "1.0.0"

from uberjob import graph, progress, stores
from uberjob._errors import CallError, NotTransformedError
from uberjob._plan import Plan
from uberjob._registry import Registry
from uberjob._rendering import render
from uberjob._run import run
from uberjob._value_store import ValueStore

__all__ = [
    "Plan",
    "Registry",
    "graph",
    "progress",
    "run",
    "render",
    "ValueStore",
    "stores",
    "CallError",
    "NotTransformedError",
]
