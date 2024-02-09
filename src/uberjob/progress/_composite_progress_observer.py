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
from collections.abc import Iterable
from contextlib import ExitStack

from uberjob.progress._progress_observer import ProgressObserver


class CompositeProgressObserver(ProgressObserver):
    def __init__(self, progress_observers: Iterable[ProgressObserver]):
        super().__init__()
        self._progress_observers = tuple(progress_observers)
        for progress_observer in self._progress_observers:
            if not isinstance(progress_observer, ProgressObserver):
                raise TypeError(
                    f"Expected a ProgressObserver, but got a {type(progress_observer)!r} instead."
                )
        self._stack = None

    def __enter__(self):
        with ExitStack() as stack:
            for progress_observer in self._progress_observers:
                stack.enter_context(progress_observer)
            self._stack = stack.pop_all()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stack.__exit__(exc_type, exc_val, exc_tb)

    def increment_total(self, *, section: str, scope: tuple, amount: int):
        for progress_observer in self._progress_observers:
            progress_observer.increment_total(
                section=section, scope=scope, amount=amount
            )

    def increment_running(self, *, section: str, scope: tuple):
        for progress_observer in self._progress_observers:
            progress_observer.increment_running(section=section, scope=scope)

    def increment_completed(self, *, section: str, scope: tuple):
        for progress_observer in self._progress_observers:
            progress_observer.increment_completed(section=section, scope=scope)

    def increment_failed(self, *, section: str, scope: tuple, exception: Exception):
        for progress_observer in self._progress_observers:
            progress_observer.increment_failed(
                section=section, scope=scope, exception=exception
            )
