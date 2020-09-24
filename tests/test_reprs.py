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

from uberjob.stores import JsonFileStore, TextFileStore


class ReprsTest(TestCase):
    def test_json_file_store_repr(self):
        path = "/example/file.json"
        value_store = JsonFileStore(path)
        self.assertEqual(repr(value_store), f"JsonFileStore({path!r})")

    def test_text_file_store_repr(self):
        path = "/example/file.txt"
        value_store = TextFileStore(path)
        self.assertEqual(repr(value_store), f"TextFileStore({path!r})")
