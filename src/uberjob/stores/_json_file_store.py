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
import json
import pathlib
import typing

from uberjob._util import repr_helper
from uberjob.stores._file_store import FileStore, staged_write


class JsonFileStore(FileStore):
    """
    A :class:`~uberjob.ValueStore` for storing a JSON-serializable value in a file.

    :param path: The path of the file.
    :param encoding: The name of the encoding used to decode or encode the file.
    """

    __slots__ = ("encoding",)

    def __init__(
        self,
        path: typing.Union[str, pathlib.Path],
        *,
        encoding: typing.Optional[str] = None
    ):
        super().__init__(path)
        self.encoding = encoding

    def read(self):
        """Read the JSON value from the file."""
        with open(self.path, encoding=self.encoding) as inputfile:
            return json.load(inputfile)

    def write(self, value) -> None:
        """
        Write a JSON-serializable value to the file.

        :param value: The value.
        """
        with staged_write(self.path, encoding=self.encoding) as outputfile:
            json.dump(value, outputfile, indent=4)

    def __repr__(self):
        return repr_helper(
            self, self.path, encoding=self.encoding, defaults={"encoding": None}
        )
