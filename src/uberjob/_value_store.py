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
import typing
from abc import ABC, abstractmethod


class ValueStore(ABC):
    """The abstract base class for all value stores."""

    __slots__ = ()

    @abstractmethod
    def read(self):
        """Read the value from the store."""

    @abstractmethod
    def write(self, value) -> None:
        """
        Write a value to the store.

        :param value: The value.
        """

    @abstractmethod
    def get_modified_time(self) -> typing.Optional[dt.datetime]:
        """Get the modified time of the stored value, or ``None`` if there is no stored value."""
