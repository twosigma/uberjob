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
from uberjob.stores._file_store import FileStore, staged_write


class BinaryFileStore(FileStore):
    """
    A :class:`~uberjob.ValueStore` for storing a ``bytes`` value in a file.

    :param path: The path.
    """

    __slots__ = ()

    def read(self) -> bytes:
        """Read the binary value from the file."""
        with open(self.path, mode="rb") as inputfile:
            return inputfile.read()

    def write(self, value: bytes) -> None:
        """
        Write a binary value to the file.

        :param value: The value.
        """
        with staged_write(self.path, mode="wb") as outputfile:
            outputfile.write(value)
