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
import html
import pathlib
import traceback
from typing import Callable, Union

from uberjob.progress._simple_progress_observer import (
    SimpleProgressObserver,
    get_scope_string,
    sorted_scope_items,
)
from uberjob.stores._file_store import staged_write

TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>\u00FCberjob</title>
        <meta name="description" contents="\u00FCberjob">
        <link rel="stylesheet"
              href="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/4.2.1/css/bootstrap.min.css"
              integrity="sha256-azvvU9xKluwHFJ0Cpgtf0CYzK7zgtOznnzxV4924X1w="
              crossorigin="anonymous"/>
    </head>
    <body>
        <div class="container">
            {body}
        </div>
        <script
            src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.3.1/jquery.slim.min.js"
            integrity="sha256-3edrmyuQ0w65f8gfBsqowzjJe2iM6n0nKciPUp8y+7E="
            crossorigin="anonymous"></script>
        <script
            src="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/4.2.1/js/bootstrap.min.js"
            integrity="sha256-63ld7aiYP6UxBifJWEzz87ldJyVnETUABZAYs5Qcsmc="
            crossorigin="anonymous"></script>
    </body>
</html>
"""


def _get_exception_lines(exception_tuples):
    lines = []
    lines.append('<h3 class="mt-4">Exceptions</h3>')
    lines.append('<div id="accordion">')
    for i, (scope, exception_tuple) in enumerate(exception_tuples):
        lines.append('  <div class="card rounded-0">')
        lines.append('    <div class="card-header p-0" id="heading{}">'.format(i))
        lines.append(
            '       <button class="btn btn-link" data-toggle="collapse" data-target="#collapse{i}"'
            ' aria-expanded="true" aria-controls="collapse{i}">'.format(i=i)
        )
        lines.append(
            "Exception {}; {}".format(i + 1, html.escape(get_scope_string(scope)))
        )
        lines.append("       </button>")
        lines.append("    </div>")
        lines.append(
            '    <div id="collapse{i}" class="collapse {show}" aria-labelledby="heading{i}"'
            ' data-parent="#accordion">'.format(show="show" if i == 0 else "", i=i)
        )
        lines.append('      <div class="card-body">')
        lines.append(
            '        <pre style="line-height: 120%">{}</pre>'.format(
                html.escape("".join(traceback.format_exception(*exception_tuple)))
            )
        )
        lines.append("        </pre>")
        lines.append("      </div>")
        lines.append("    </div>")
        lines.append("  </div>")
    lines.append("</div>")
    return lines


class HtmlProgressObserver(SimpleProgressObserver):
    """An observer that writes progress to an HTML file."""

    def __init__(
        self,
        output: Union[str, pathlib.Path, Callable[[bytes], None]],
        *,
        min_update_interval,
        max_update_interval
    ):
        super().__init__(
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
        lines = [
            '<div class="d-flex mt-4 align-items-end">',
            '  <div class="mr-auto"><h1>\u00FCberjob</h1></div>',
            '  <div class="mr-4"><h6>Elapsed {}</h6></div>'.format(
                html.escape(str(dt.timedelta(seconds=int(elapsed))))
            ),
            "  <div><h6>Updated {}</h6></div>".format(
                html.escape(dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
            ),
            "</div>",
        ]
        for section, title in (
            ("stale", "Determining stale value stores"),
            ("run", "Running graph"),
        ):
            scope_mapping = state.section_scope_mapping.get(section)
            if scope_mapping:
                lines.append('<h3 class="mt-4">{}</h3>'.format(html.escape(title)))
                for scope, scope_state in sorted_scope_items(scope_mapping):
                    completed_percentage = (
                        100 * scope_state.completed / scope_state.total
                    )
                    running_percentage = 100 * scope_state.running / scope_state.total
                    failed_percentage = 100 * scope_state.failed / scope_state.total
                    remaining_percentage = (
                        100
                        - completed_percentage
                        - running_percentage
                        - failed_percentage
                    )
                    failure_indicator_percentage = (
                        remaining_percentage if scope_state.failed else 0
                    )
                    lines.append(
                        """\
                    <div class="row align-items-center pt-1 pb-1 ml-0 mr-0 border">
                        <div class="col"><samp>{scope_string}</samp></div>
                        <div class="col-2 text-right">{progress_string}</div>
                        <div class="col-3">
                            <div class="progress">
                                <div class="progress-bar {completed_class}"
                                     role="progressbar" style="width:{completed}%"></div>
                                <div class="progress-bar bg-warning progress-bar-striped progress-bar-animated"
                                     role="progressbar" style="width:{running}%"></div>
                                <div class="progress-bar bg-danger"
                                     role="progressbar" style="width:{failed}%"></div>
                                <div class="progress-bar bg-secondary"
                                     role="progressbar" style="width:{failure_indicator}%"></div>
                            </div>
                        </div>
                    </div>
                    """.format(
                            progress_string=html.escape(
                                scope_state.to_progress_string()
                            ),
                            scope_string=html.escape(
                                get_scope_string(scope, add_zero_width_spaces=True)
                            ),
                            completed=completed_percentage,
                            running=running_percentage,
                            failed=failed_percentage,
                            failure_indicator=failure_indicator_percentage,
                            completed_class="bg-success"
                            if scope_state.completed == scope_state.total
                            else "",
                        )
                    )

        if exception_tuples:
            lines.extend(_get_exception_lines(exception_tuples))
        return TEMPLATE.format(body="\n".join(lines)).encode("utf8")

    def _output(self, value):
        self._output_fn(value)
