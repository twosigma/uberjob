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
import pathlib
import typing

from uberjob._util import repr_helper
from uberjob._value_store import ValueStore
from uberjob.stores._file_store import get_modified_time


class PathSource(ValueStore):
    """
    A :class:`~uberjob.ValueStore` that returns the path itself from ``read`` rather than actually reading any data.

    :param path: The input path.
    :param required: When true, ``get_modified_time`` will raise an exception when the path is missing rather than
                     return ``None``.
    """

    def __init__(self, path: typing.Union[str, pathlib.Path], *, required: bool = True):
        self.path = path
        self.required = required

    def read(self) -> typing.Union[str, pathlib.Path]:
        """
        Get the path.

        When ``required`` is false, this will raise an exception if the file does not exist.
        """
        if not self.required:
            self._get_modified_time(required=True)
        return self.path

    def write(self, value):
        """Not implemented."""
        raise NotImplementedError()

    def get_modified_time(self) -> typing.Optional[dt.datetime]:
        """
        Get the modified time of the file.

        If it does not exist or is inaccessible, ``None`` will be returned if ``required`` is false and an exception
        will be raised otherwise.
        """
        return self._get_modified_time(self.required)

    def _get_modified_time(self, required):
        modified_time = get_modified_time(self.path)
        if modified_time is None and required:
            raise IOError(
                f"Failed to get modified time of required source path {self.path!r}."
            )
        return modified_time

    def __repr__(self):
        return repr_helper(
            self, self.path, required=self.required, defaults={"required": True}
        )
