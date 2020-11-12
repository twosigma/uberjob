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
import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from typing import Tuple

from uberjob.progress._progress_observer import ProgressObserver


def _universal_sort_key(*args):
    return tuple((str(type(x)), x) for x in args)


def sorted_scope_items(scope_dict):
    return sorted(scope_dict.items(), key=lambda pair: _universal_sort_key(*pair[0]))


def get_scope_string(scope, *, add_zero_width_spaces=False):
    scope_string = ", ".join(str(value) for value in scope)
    if add_zero_width_spaces:
        scope_string = scope_string.replace(".", "\u200B.")
    return scope_string


def _get_progress_string(*, completed, failed, running, total, weighted_elapsed):
    all_done = completed + failed == total
    started = completed + failed + running > 0
    if all_done or not started:
        progress_string = f"{completed} / {total}"
    else:
        progress_string = f"({completed} + {running}) / {total}"
    if failed:
        progress_string = f"{progress_string}; {failed} failed"
    weighted_elapsed = int(weighted_elapsed)
    if weighted_elapsed:
        progress_string = f"{progress_string}; {dt.timedelta(seconds=weighted_elapsed)}"
    return progress_string


class ScopeState:
    def __init__(self):
        self.completed = 0
        self.failed = 0
        self.running = 0
        self.total = 0
        self.weighted_elapsed = 0

    def to_progress_string(self):
        return _get_progress_string(
            completed=self.completed,
            failed=self.failed,
            running=self.running,
            total=self.total,
            weighted_elapsed=self.weighted_elapsed,
        )


class State:
    def __init__(self):
        self.section_scope_mapping = defaultdict(lambda: defaultdict(ScopeState))
        self.running_count = 0
        self._running_section_scopes = set()
        self._prev_time = None

    def increment_total(self, section, scope, amount: int):
        self.section_scope_mapping[section][scope].total += amount

    def increment_running(self, section, scope):
        self.update_weighted_elapsed()
        scope_state = self.section_scope_mapping[section][scope]
        if not scope_state.running:
            self._running_section_scopes.add((section, scope))
        scope_state.running += 1
        self.running_count += 1

    def increment_completed(self, section, scope):
        self.update_weighted_elapsed()
        scope_state = self.section_scope_mapping[section][scope]
        scope_state.running -= 1
        self.running_count -= 1
        if not scope_state.running:
            self._running_section_scopes.remove((section, scope))
        scope_state.completed += 1

    def increment_failed(self, section, scope):
        self.update_weighted_elapsed()
        scope_state = self.section_scope_mapping[section][scope]
        scope_state.running -= 1
        self.running_count -= 1
        if not scope_state.running:
            self._running_section_scopes.remove((section, scope))
        scope_state.failed += 1

    def update_weighted_elapsed(self):
        t = time.time()
        if self._prev_time:
            elapsed = t - self._prev_time
            if self.running_count:
                for section, scope in self._running_section_scopes:
                    scope_state = self.section_scope_mapping[section][scope]
                    scope_state.weighted_elapsed += (
                        elapsed * scope_state.running / self.running_count
                    )
        self._prev_time = t


class SimpleProgressObserver(ProgressObserver, ABC):
    def __init__(
        self, *, min_update_interval, max_update_interval, max_exception_count=128
    ):
        self._min_update_interval = min_update_interval
        self._max_update_interval = max_update_interval
        self._max_exception_count = max_exception_count
        self._state = State()
        self._running_scope_lookup = defaultdict(set)
        self._exception_tuples = []
        self._new_exception_index = 0
        self._lock = threading.Lock()
        self._stale = True
        self._done_event = threading.Event()
        self._thread = None
        self._start_time = None
        self._last_render_time = None

    @abstractmethod
    def _render(self, state, new_exception_index, exception_tuples, elapsed):
        pass

    @abstractmethod
    def _output(self, value):
        pass

    def __enter__(self):
        self._thread = threading.Thread(target=self._run_update_thread)
        self._thread.start()
        self._start_time = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._done_event.set()
        self._thread.join()
        self._thread = None

    def _do_render(self):
        t = time.time()
        if (
            self._stale
            or self._last_render_time is None
            or t - self._last_render_time >= self._max_update_interval
        ):
            self._stale = False
            self._last_render_time = t
            self._state.update_weighted_elapsed()
            output_value = self._render(
                self._state,
                self._new_exception_index,
                self._exception_tuples,
                t - self._start_time,
            )
            self._new_exception_index = len(self._exception_tuples)
            return output_value
        return None

    def _run_update_thread(self):
        done = False
        while not done:
            done = self._done_event.wait(self._min_update_interval)
            with self._lock:
                output_value = self._do_render()
            if output_value is not None:
                self._output(output_value)

    @contextmanager
    def _lock_and_make_stale(self):
        with self._lock:
            self._stale = True
            yield

    def increment_total(self, *, section: str, scope: Tuple, amount: int):
        with self._lock_and_make_stale():
            self._state.increment_total(section, scope, amount)

    def increment_running(self, *, section: str, scope: Tuple):
        with self._lock_and_make_stale():
            self._state.increment_running(section, scope)

    def increment_completed(self, *, section: str, scope: Tuple):
        with self._lock_and_make_stale():
            self._state.increment_completed(section, scope)

    def increment_failed(self, *, section: str, scope: Tuple, exception: Exception):
        with self._lock_and_make_stale():
            self._state.increment_failed(section, scope)
            if len(self._exception_tuples) < self._max_exception_count:
                self._exception_tuples.append(
                    (scope, (type(exception), exception, exception.__traceback__))
                )
