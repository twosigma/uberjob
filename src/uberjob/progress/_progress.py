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
from collections.abc import Callable

from uberjob.progress._progress_observer import ProgressObserver


class Progress:
    """
    A way to observe progress.

    :param create_observer: A callable that creates a single-use progress observer.
    """

    def __init__(self, create_observer: Callable[[], ProgressObserver]):
        self._create_observer = create_observer

    def observer(self) -> ProgressObserver:
        """Create a single-use progress observer."""
        return self._create_observer()
