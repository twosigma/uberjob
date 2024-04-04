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

from uberjob._testing.test_store import TestStore
from uberjob._util import repr_helper
from uberjob.stores._file_store import FileStore
from uberjob.stores._mounted_store import MountedStore


class TestMountedFileStore(MountedStore):
    def __init__(self, create_file_store: typing.Callable[[str], FileStore]):
        super().__init__(create_file_store)
        self.remote_store = TestStore()

    def copy_from_local(self, local_path):
        with open(local_path, "rb") as f:
            self.remote_store.write(f.read())

    def copy_to_local(self, local_path):
        with open(local_path, "wb") as f:
            f.write(self.remote_store.read())

    def get_modified_time(self) -> dt.datetime | None:
        return self.remote_store.get_modified_time()

    def __repr__(self):
        return repr_helper(self, self.create_store, self.remote_store)
