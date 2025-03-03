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
from abc import ABC, abstractmethod


class AsyncValueStore(ABC):
    """The abstract base class for all async value stores."""

    __slots__ = ()

    @abstractmethod
    async def read(self):
        """Read the value from the store asynchronously."""

    @abstractmethod
    async def write(self, value) -> None:
        """
        Write a value to the store asynchronously.

        :param value: The value.
        """

    @abstractmethod
    async def get_modified_time(self) -> dt.datetime | None:
        """Get the modified time of the stored value asynchronously, or ``None`` if there is no stored value."""
