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
import os
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Callable
from contextlib import contextmanager

from uberjob._util import repr_helper
from uberjob._value_store import ValueStore


@contextmanager
def _path_context():
    with tempfile.TemporaryDirectory() as tempdir:
        yield os.path.join(tempdir, "temp")


class MountedStore(ValueStore, ABC):
    """
    An abstract :class:`~uberjob.ValueStore` that behaves like a mounted storage device.

    :param create_store: A callable that creates a :class:`~uberjob.ValueStore` for a path.
    """

    __slots__ = ("create_store",)

    def __init__(self, create_store: Callable[[str], ValueStore]):
        self.create_store = create_store
        """Creates an instance of the underlying :class:`~uberjob.ValueStore` for the given path."""

    @abstractmethod
    def copy_to_local(self, local_path: str):
        """
        Copy the value at the local_path into the store.

        :param local_path: The local path.
        """

    @abstractmethod
    def copy_from_local(self, local_path: str):
        """
        Copy the value in the store to the local_path.

        :param local_path: The local path.
        """

    def read(self):
        with _path_context() as local_path:
            self.copy_to_local(local_path)
            return self.create_store(local_path).read()

    def write(self, value) -> None:
        with _path_context() as local_path:
            self.create_store(local_path).write(value)
            self.copy_from_local(local_path)

    def __repr__(self):
        return repr_helper(self, self.create_store)
