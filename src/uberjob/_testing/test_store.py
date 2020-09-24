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

from uberjob._util import Missing, repr_helper
from uberjob._value_store import ValueStore


class TestStore(ValueStore):
    """
    A value store that for use with _testing, that gives the user full control over its state.

    :param value: The initial value in the value store. Defaults to an empty value store.
    :param modified_time: The modified time of the value store. Defaults to None if value store is empty or the
                          current time if a value is provided.
    :param can_read: Determines whether the value store is allowed to read. Defaults to True.
    :param can_write: Determines whether the value store is allowed to write. Defaults to True.
    :param read_count: The number of times read has been called. Defaults to 0.
    :param write_count: The number of times write has been called. Defaults to 0.
    """

    def __init__(
        self,
        value=Missing,
        *,
        modified_time=Missing,
        can_read=True,
        can_write=True,
        can_get_modified_time=True,
        read_count=0,
        write_count=0
    ):
        if modified_time is not Missing and (value is Missing) != (
            modified_time is None
        ):
            raise ValueError(
                "If a modified_time is supplied, a value must be supplied as well."
            )
        self.value = value
        self.modified_time = (
            None
            if value is Missing
            else dt.datetime.utcnow()
            if modified_time is Missing
            else modified_time
        )
        self.can_read = can_read
        self.can_write = can_write
        self.can_get_modified_time = can_get_modified_time
        self.read_count = read_count
        self.write_count = write_count

    def read(self):
        if not self.can_read:
            raise Exception("This test store cannot read.")
        self.read_count += 1
        if self.value is Missing:
            raise Exception("Failed to read value from empty store.")
        return self.value

    def write(self, value):
        if not self.can_write:
            raise Exception("This test store cannot write.")
        self.write_count += 1
        self.value = value
        self.modified_time = dt.datetime.utcnow()

    def get_modified_time(self) -> typing.Optional[dt.datetime]:
        if not self.can_get_modified_time:
            raise Exception("This test store cannot get modified time.")
        return self.modified_time

    def clear(self):
        self.value = Missing
        self.modified_time = None
        self.read_count = 0
        self.write_count = 0

    def __repr__(self):
        return repr_helper(
            self,
            self.value,
            modified_time=self.modified_time.isoformat()
            if self.modified_time
            else None,
            can_read=self.can_read,
            can_write=self.can_write,
            read_count=self.read_count,
            write_count=self.write_count,
            defaults={
                0: Missing,
                "modified_time": None,
                "can_read": True,
                "can_write": True,
                "read_count": 0,
                "write_count": 0,
            },
        )
