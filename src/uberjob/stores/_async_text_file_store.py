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
import pathlib

import aiofiles

from uberjob._util import repr_helper
from uberjob.stores._async_file_store import AsyncFileStore, async_staged_write


class AsyncTextFileStore(AsyncFileStore):
    """
    An async :class:`~uberjob.AsyncValueStore` for storing a ``str`` value in a file.

    :param path: The path of the file.
    :param encoding: The name of the encoding used to decode or encode the file.
    """

    __slots__ = ("encoding",)

    def __init__(self, path: str | pathlib.Path, *, encoding: str | None = None):
        super().__init__(path)
        self.encoding = encoding

    async def read(self) -> str:
        """Read the string value from the file asynchronously."""
        async with aiofiles.open(self.path, encoding=self.encoding) as inputfile:
            return await inputfile.read()

    async def write(self, value: str) -> None:
        """
        Write a string value to the file asynchronously.

        :param value: The value.
        """
        async with async_staged_write(self.path, encoding=self.encoding) as outputfile:
            await outputfile.write(value)

    def __repr__(self):
        return repr_helper(
            self, self.path, encoding=self.encoding, defaults={"encoding": None}
        )
