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
import textwrap
import traceback
from functools import partial
from io import StringIO

from uberjob.progress._simple_progress_observer import (
    SimpleProgressObserver,
    get_elapsed_string,
    get_scope_string,
    sorted_scope_items,
)


def _print_header(print_, elapsed):
    print_(
        "uberjob, elapsed {}, updated {}".format(
            get_elapsed_string(elapsed),
            dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        )
    )


def _ralign(strings):
    strings = list(strings)
    max_length = max(len(s) for s in strings)
    return [s.rjust(max_length) for s in strings]


def _print_section(print_, section, scope_mapping):
    scope_items = sorted_scope_items(scope_mapping)
    print_("{}:".format(section))
    progress_strs = _ralign(
        scope_state.to_progress_string() for scope, scope_state in scope_items
    )
    elapsed_strs = _ralign(
        scope_state.to_elapsed_string() for scope, scope_state in scope_items
    )
    for progress_str, elapsed_str, (scope, _scope_state) in zip(
        progress_strs, elapsed_strs, scope_items
    ):
        print_(
            "  {} | {} | {}".format(progress_str, elapsed_str, get_scope_string(scope))
        )


def _print_new_exceptions(print_, new_exception_index, exception_tuples):
    if new_exception_index < len(exception_tuples):
        print_("new exceptions:")
        for exception_index in range(new_exception_index, len(exception_tuples)):
            scope, exception_tuple = exception_tuples[exception_index]
            print_(
                "  exception {}; {}".format(
                    exception_index + 1, get_scope_string(scope)
                )
            )
            print_(
                textwrap.indent(
                    "".join(traceback.format_exception(*exception_tuple)),
                    prefix=" " * 4,
                )
            )


class ConsoleProgressObserver(SimpleProgressObserver):
    """An observer that prints progress to the console."""

    def __init__(
        self, *, initial_update_delay, min_update_interval, max_update_interval
    ):
        super().__init__(
            initial_update_delay=initial_update_delay,
            min_update_interval=min_update_interval,
            max_update_interval=max_update_interval,
        )
        self._skipped_sections = set()

    def _render(self, state, new_exception_index, exception_tuples, elapsed):
        with StringIO() as buffer:
            print_ = partial(print, file=buffer)
            _print_header(print_, elapsed)
            for section in ("stale", "run"):
                scope_mapping = state.get(section)
                if scope_mapping:
                    is_done = all(
                        s.completed + s.failed == s.total
                        for s in scope_mapping.values()
                    )
                    if not is_done or section not in self._skipped_sections:
                        _print_section(print_, section, scope_mapping)
                    if is_done:
                        self._skipped_sections.add(section)
                    else:
                        self._skipped_sections.discard(section)
            _print_new_exceptions(print_, new_exception_index, exception_tuples)
            return buffer.getvalue()

    def _output(self, value):
        print(value, end="", flush=True)
