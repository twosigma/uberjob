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
import datetime as dt
import os
import pathlib
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import IO, AnyStr, ContextManager, Optional, Union

from uberjob._util import repr_helper
from uberjob._value_store import ValueStore

STAGING_SUFFIX = ".STAGING"


def get_modified_time(path: Union[str, pathlib.Path]) -> Optional[dt.datetime]:
    """
    Gets the modified time of the path, or ``None`` if it does not exist or is inaccessible.

    :param path: The path.
    """
    try:
        t = os.path.getmtime(path)
    except OSError:
        return None
    return dt.datetime.fromtimestamp(t)


def _try_remove(path):
    try:
        os.remove(path)
    except OSError:
        pass


@contextmanager
def staged_write_path(
    path: Union[str, pathlib.Path]
) -> ContextManager[Union[str, pathlib.Path]]:
    """
    Context manager for writing a file atomically.

    It yields a staging path which will be atomically renamed to the given path if an exception is not raised.
    If an exception is raised, the staging path will be deleted if it exists.

    :param path: The path.
    """
    staging_path = f"{path}.STAGING"
    if isinstance(path, pathlib.Path):
        staging_path = pathlib.Path(staging_path)
    try:
        yield staging_path
    except BaseException:
        _try_remove(staging_path)
        raise
    os.replace(staging_path, path)


@contextmanager
def staged_write(
    path: Union[str, pathlib.Path], mode="w", **kwargs
) -> ContextManager[IO[AnyStr]]:
    """
    Context manager for writing a file atomically.

    It yields a staging file object which will be atomically renamed to the given path if an exception is not raised.
    If an exception is raised, the staging file will be deleted if it exists.

    :param path: The path.
    :param mode: The file open mode.
    :param kwargs: Extra arguments to pass to :func:`open`.
    """
    if "w" not in mode:
        raise ValueError("The mode must include 'w'")
    with staged_write_path(path) as staging_path:
        with open(staging_path, mode, **kwargs) as outputfile:
            yield outputfile


class FileStore(ValueStore, ABC):
    """
    The abstract base class for storing a value in a file.

    :param path: The path.
    """

    def __init__(self, path: Union[str, pathlib.Path]):
        self.path = path

    @abstractmethod
    def read(self):
        """Read the value from the file."""

    @abstractmethod
    def write(self, value) -> None:
        """
        Write a value to the file.

        :param value: The value.
        """

    def get_modified_time(self) -> Optional[dt.datetime]:
        """Get the modified time of the file, or ``None`` if it does not exist or is inaccessible."""
        return get_modified_time(self.path)

    def __repr__(self):
        return repr_helper(self, self.path)
