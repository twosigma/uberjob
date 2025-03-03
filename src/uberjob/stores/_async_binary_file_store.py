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
import aiofiles

from uberjob.stores._async_file_store import AsyncFileStore, async_staged_write


class AsyncBinaryFileStore(AsyncFileStore):
    """
    An async :class:`~uberjob.AsyncValueStore` for storing a ``bytes`` value in a file.

    :param path: The path.
    """

    __slots__ = ()

    async def read(self) -> bytes:
        """Read the binary value from the file asynchronously."""
        async with aiofiles.open(self.path, mode="rb") as inputfile:
            return await inputfile.read()

    async def write(self, value: bytes) -> None:
        """
        Write a binary value to the file asynchronously.

        :param value: The value.
        """
        async with async_staged_write(self.path, mode="wb") as outputfile:
            await outputfile.write(value)
