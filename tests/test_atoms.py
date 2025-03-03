#
# Copyright 2025 Two Sigma Open Source, LLC
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

import pickle
import unittest

from uberjob._transformations.caching import Barrier, BarrierType
from uberjob._util import Missing, MissingType, Omitted, OmittedType
from uberjob._util.traceback import TruncatedStackFrame, TruncatedStackFrameType

ATOMS = [
    (BarrierType, Barrier, "Barrier"),
    (MissingType, Missing, "Missing"),
    (OmittedType, Omitted, "<...>"),
    (TruncatedStackFrameType, TruncatedStackFrame, "TruncatedStackFrame"),
]


class AtomTestCase(unittest.TestCase):
    def test_atom_singleton(self):
        for atom_type, atom, _ in ATOMS:
            with self.subTest(atom_type_name=atom_type.__name__):
                self.assertIs(atom_type(), atom_type())
                self.assertIs(atom_type(), atom)

    def test_atom_pickle_round_trip_is_atom(self):
        for atom_type, atom, _ in ATOMS:
            with self.subTest(atom_type_name=atom_type.__name__):
                self.assertIs(pickle.loads(pickle.dumps(atom)), atom)

    def test_type_of_atom_is_atom_type(self):
        for atom_type, atom, _ in ATOMS:
            with self.subTest(atom_type_name=atom_type.__name__):
                self.assertIs(type(atom), atom_type)

    def test_atom_repr(self):
        for atom_type, atom, atom_repr in ATOMS:
            with self.subTest(atom_type_name=atom_type.__name__):
                self.assertEqual(repr(atom), atom_repr)
