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

from uberjob._util import repr_helper
from uberjob._value_store import ValueStore


class LiteralSource(ValueStore):
    """
    A :class:`~uberjob.ValueStore` that takes value and modified time in its constructor
    and simply returns them from ``read`` and ``get_modified_time``.

    :param value: The value.
    :param modified_time: The modified time.
    """

    __slots__ = ("value", "modified_time")

    def __init__(self, value, modified_time: typing.Optional[dt.datetime]):
        self.value = value
        self.modified_time = modified_time

    def read(self):
        """Get the value."""
        return self.value

    def write(self, value):
        """Not implemented."""
        raise NotImplementedError()

    def get_modified_time(self) -> typing.Optional[dt.datetime]:
        """Get the modified time."""
        return self.modified_time

    def __repr__(self):
        return repr_helper(
            self,
            self.value,
            self.modified_time.isoformat() if self.modified_time else None,
        )
