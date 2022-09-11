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


class TouchFileStore(FileStore):
    """
    A :class:`~uberjob.ValueStore` for a touch file. It can be thought of as storing ``None`` in a file.

    It is useful for integrating side effects.

    :param path: The path.
    """

    __slots__ = ()

    def read(self):
        """Return ``None`` after ensuring that the touch file exists and is empty."""
        with open(self.path, "rb") as inputfile:
            if inputfile.read(1):
                raise IOError(f"The path {self.path!r} exists but is not empty.")
        return None

    def write(self, value: None) -> None:
        """
        Write the touch file after ensuring that the given value is ``None``.

        :param value: The value, which must be ``None``.
        """
        if value is not None:
            raise TypeError("The value must be None.")
        with staged_write(self.path, "wb"):
            pass
