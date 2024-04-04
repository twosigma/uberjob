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
import pathlib
import traceback
from collections.abc import Callable
from html import escape

from uberjob.progress._simple_progress_observer import (
    ScopeState,
    SimpleProgressObserver,
    get_elapsed_string,
    get_scope_string,
    sorted_scope_items,
)
from uberjob.stores._file_store import staged_write


def _render_html(state, exception_tuples, elapsed):
    return f"""
        <!DOCTYPE html>
        <html lang="en">
          <head>
            <meta charset="UTF-8">
            <title>uberjob</title>
            <meta name="description" contents="uberjob">
            <link
                href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css"
                rel="stylesheet"
                integrity="sha384-T3c6CoIi6uLrA9TneNEoa7RxnatzjcDSCmG1MXxSR1GAsXEV/Dwwykc2MPK8M2HN"
                crossorigin="anonymous">
          </head>
          <body>
            {_render_body(state, exception_tuples, elapsed)}
            <script
              src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"
              integrity="sha384-C6RzsynM9kWDrMNeT87bh95OGNyZPhcTNXj1NW7RuBCsyN/o0jlpcV8Qyq46cDfL"
              crossorigin="anonymous"></script>
          </body>
        </html>
        """


def _render_body(state, exception_tuples, elapsed):
    rendered_sections = "\n".join(
        _render_section(title, scope_mapping)
        for section, title in (
            ("stale", "Determining stale value stores"),
            ("run", "Running graph"),
        )
        if (scope_mapping := state.get(section))
    )
    rendered_exception_tuples = ""
    if exception_tuples:
        rendered_exception_tuples = _render_exception_tuples(exception_tuples)
    return f"""
        <div class="container-fluid">
          <div class="d-flex mt-4 align-items-end">
            <div class="me-auto">
              <h1>uberjob</h1>
            </div>
            <div class="me-4">
              <h6>Elapsed {escape(get_elapsed_string(elapsed))}</h6>
            </div>
            <div>
              <h6>Updated {escape(dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))}</h6>
            </div>
          </div>
          {rendered_sections}
          {rendered_exception_tuples}
        </div>
        """


def _get_total_scope_state(scope_states):
    return ScopeState(
        completed=sum(s.completed for s in scope_states),
        failed=sum(s.failed for s in scope_states),
        running=sum(s.running for s in scope_states),
        total=sum(s.total for s in scope_states),
        weighted_elapsed=sum(s.weighted_elapsed for s in scope_states),
    )


def _render_section(title, scope_mapping):
    rendered_scopes = "\n".join(
        _render_scope(scope, scope_state)
        for scope, scope_state in sorted_scope_items(scope_mapping)
    )
    rendered_footer = ""
    if len(scope_mapping) > 1:
        total_scope_state = _get_total_scope_state(scope_mapping.values())
        rendered_footer = f"""
            <tfoot class="table-group-divider">
              {_render_scope(["Total"], total_scope_state, is_bold=True)}
            </tfoot>
            """
    return f"""
        <h3 class="mt-4">{escape(title)}</h3>
        <table class="table table-sm table-striped table-auto">
          <thead>
            <tr>
              <th scope="col" style="width: 10%"></th>
              <th scope="col" class="text-end">Progress</th>
              <th scope="col" class="text-end">Elapsed</th>
              <th scope="col">Scope</th>
            </tr>
          </thead>
          <tbody class="table-group-divider">
            {rendered_scopes}
          </tbody>
          {rendered_footer}
        </table>
        """


def _get_html_progress_string(scope_state, *, is_failed_bold=False):
    completed = scope_state.completed
    failed = scope_state.failed
    running = scope_state.running
    total = scope_state.total
    all_done = completed + failed == total
    started = completed + failed + running > 0
    if all_done or not started:
        progress_string = escape(f"{completed} / {total}")
    else:
        progress_string = escape(f"({completed} + {running}) / {total}")
    if failed:
        failed_string = (
            f'<span class="badge text-bg-danger '
            f'{"fw-bolder" if is_failed_bold else ""}">{escape(str(failed))} failed</span>'
        )
        progress_string = f"{progress_string}, {failed_string}"
    return progress_string


def _render_scope(scope, scope_state, *, is_bold=False):
    completed_percentage = 100 * scope_state.completed / scope_state.total
    running_percentage = 100 * scope_state.running / scope_state.total
    failed_percentage = 100 * scope_state.failed / scope_state.total
    return f"""
        <tr class="{"fw-bold" if is_bold else ""}">
          <td>
            <div class="progress">
              <div class="progress-bar {"bg-success" if scope_state.completed == scope_state.total else ""}"
                   role="progressbar" style="width:{completed_percentage}%"></div>
              <div class="progress-bar progress-bar-striped progress-bar-animated bg-warning"
                   role="progressbar" style="width:{running_percentage}%"></div>
              <div class="progress-bar bg-danger"
                   role="progressbar" style="width:{failed_percentage}%"></div>
            </div>
          </td>
          <td class="text-end">{_get_html_progress_string(scope_state, is_failed_bold=is_bold)}</td>
          <td class="text-end">{escape(scope_state.to_elapsed_string())}</td>
          <td>{escape(get_scope_string(scope, add_zero_width_spaces=True))}</td>
        </tr>
        """


def _render_exception_tuples(exception_tuples):
    rendered_exceptions = "\n".join(
        _render_exception_tuple(i, scope, exception_tuple)
        for i, (scope, exception_tuple) in enumerate(exception_tuples)
    )
    return f"""
        <h3 class="mt-4">Exceptions</h3>
        <div class="accordion" id="accordion0">
          {rendered_exceptions}
        </div>
        """


def _render_exception_tuple(i, scope, exception_tuple):
    show = "show" if i == 0 else ""
    collapsed = "collapsed" if i != 0 else ""
    exception_tuple_str = "".join(traceback.format_exception(*exception_tuple))
    return f"""
        <div class="accordion-item">
          <h2 class="accordion-header" id="heading{i}">
            <button class="accordion-button p-2 {collapsed}" type="button"
                    data-bs-toggle="collapse" data-bs-target="#collapse{i}">
              Exception {i+1}; {escape(get_scope_string(scope))}
            </button>
          </h2>
          <div id="collapse{i}" class="accordion-collapse collapse {show}" data-bs-parent="#accordion0">
            <div class="accordion-body">
              <pre style="line-height: 120%">{escape(exception_tuple_str)}</pre>
            </div>
          </div>
        </div>
        """


class HtmlProgressObserver(SimpleProgressObserver):
    """An observer that writes progress to an HTML file."""

    def __init__(
        self,
        output: str | pathlib.Path | Callable[[bytes], None],
        *,
        initial_update_delay,
        min_update_interval,
        max_update_interval,
    ):
        super().__init__(
            initial_update_delay=initial_update_delay,
            min_update_interval=min_update_interval,
            max_update_interval=max_update_interval,
        )
        if not callable(output):
            path = output

            def output(html_bytes: bytes):
                with staged_write(path, mode="wb") as f:
                    f.write(html_bytes)

        self._output_fn = output

    def _render(self, state, new_exception_index, exception_tuples, elapsed):
        return _render_html(state, exception_tuples, elapsed).encode()

    def _output(self, value):
        self._output_fn(value)
