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
from uberjob.progress._progress_observer import ProgressObserver


class NullProgressObserver(ProgressObserver):
    """A progress observer that does nothing."""

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def increment_total(self, *, section: str, scope, amount: int):
        pass

    def increment_running(self, *, section: str, scope):
        pass

    def increment_completed(self, *, section: str, scope):
        pass

    def increment_failed(self, *, section: str, scope, exception):
        pass
