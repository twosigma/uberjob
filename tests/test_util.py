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

from uberjob._builtins import gather_dict, gather_list, gather_set, gather_tuple, source
from uberjob._util import fully_qualified_name


def function1():
    pass


class Widget:
    def function2(self):
        pass


class Function3:
    def __call__(self):
        pass


class UtilTestCase(TestCase):
    def test_fully_qualified_name(self):
        cases = [
            (str.join, "str.join"),
            (function1, "{}.{}".format(self.__module__, "function1")),
            (Widget().function2, "{}.{}".format(self.__module__, "Widget.function2")),
            (Function3(), "{}.{}".format(self.__module__, "Function3")),
            *(
                (fn, fn.__name__)
                for fn in [
                    pow,
                    gather_list,
                    gather_tuple,
                    gather_set,
                    gather_dict,
                    source,
                ]
            ),
        ]
        for fn, expected in cases:
            with self.subTest(fn=fn, expected=expected):
                self.assertEqual(fully_qualified_name(fn), expected)
