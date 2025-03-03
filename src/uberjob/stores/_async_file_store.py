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
import datetime as dt
import os
import pathlib
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import IO, AnyStr

import aiofiles

from uberjob._async_value_store import AsyncValueStore
from uberjob._util import repr_helper
from uberjob.stores._file_store import STAGING_SUFFIX, get_modified_time


async def async_get_modified_time(path: str | pathlib.Path) -> dt.datetime | None:
    """
    Gets the modified time of the path asynchronously, or ``None`` if it does not exist or is inaccessible.

    :param path: The path.
    """
    # We can use the synchronous version since os.path.getmtime is very fast
    # and doesn't benefit much from async
    return get_modified_time(path)


async def _async_try_remove(path):
    """Try to remove a file asynchronously, ignoring errors."""
    try:
        os.remove(path)  # No async advantage for this operation
    except OSError:
        pass


@asynccontextmanager
async def async_staged_write_path(
    path: str | pathlib.Path,
) -> pathlib.Path:
    """
    Async context manager for writing a file atomically.

    It yields a staging path which will be atomically renamed to the given path if an exception is not raised.
    If an exception is raised, the staging path will be deleted if it exists.

    :param path: The path.
    """
    staging_path = f"{path}{STAGING_SUFFIX}"
    if isinstance(path, pathlib.Path):
        staging_path = pathlib.Path(staging_path)
    try:
        yield staging_path
    except BaseException:
        await _async_try_remove(staging_path)
        raise
    os.replace(staging_path, path)  # No async advantage for this operation


@asynccontextmanager
async def async_staged_write(
    path: str | pathlib.Path, mode="w", **kwargs
) -> IO[AnyStr]:
    """
    Async context manager for writing a file atomically.

    It yields a staging file object which will be atomically renamed to the given path if an exception is not raised.
    If an exception is raised, the staging file will be deleted if it exists.

    :param path: The path.
    :param mode: The file open mode.
    :param kwargs: Extra arguments to pass to :func:`aiofiles.open`.
    """
    if "w" not in mode:
        raise ValueError("The mode must include 'w'")
    async with async_staged_write_path(path) as staging_path:
        async with aiofiles.open(staging_path, mode, **kwargs) as outputfile:
            yield outputfile


class AsyncFileStore(AsyncValueStore, ABC):
    """
    The abstract base class for storing a value in a file asynchronously.

    :param path: The path.
    """

    __slots__ = ("path",)

    def __init__(self, path: str | pathlib.Path):
        self.path = path

    @abstractmethod
    async def read(self):
        """Read the value from the file asynchronously."""

    @abstractmethod
    async def write(self, value) -> None:
        """
        Write a value to the file asynchronously.

        :param value: The value.
        """

    async def get_modified_time(self) -> dt.datetime | None:
        """Get the modified time of the file asynchronously, or ``None`` if it does not exist or is inaccessible."""
        return await async_get_modified_time(self.path)

    def __repr__(self):
        return repr_helper(self, self.path)
