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
import pickle

from uberjob.stores._file_store import FileStore, staged_write


class PickleFileStore(FileStore):
    """
    A :class:`~uberjob.ValueStore` for storing a picklable value in a file.

    :param path: The path.
    """

    __slots__ = ()

    def read(self):
        """Read the pickled value from the file."""
        with open(self.path, "rb") as inputfile:
            return pickle.load(inputfile)

    def write(self, value) -> None:
        """
        Write a picklable value to the file.

        :param value: The value.
        """
        with staged_write(self.path, "wb") as outputfile:
            pickle.dump(value, outputfile)
