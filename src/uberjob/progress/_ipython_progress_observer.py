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
import traceback

from uberjob.progress._simple_progress_observer import (
    SimpleProgressObserver,
    get_scope_string,
    sorted_scope_items,
)


class IPythonProgressObserver(SimpleProgressObserver):
    """An observer that displays progress in a Jupyter Notebook using IPython Widgets."""

    def __init__(
        self, *, initial_update_delay, min_update_interval, max_update_interval
    ):
        super().__init__(
            initial_update_delay=initial_update_delay,
            min_update_interval=min_update_interval,
            max_update_interval=max_update_interval,
        )
        self._widget_cache = None

    def _get(self, *key, default):
        key = tuple(key)
        widget = self._widget_cache.get(key)
        if widget is None:
            self._widget_cache[key] = widget = default()
        return widget

    def _render(self, state, new_exception_index, exception_tuples, elapsed):
        import ipywidgets as widgets
        from IPython.display import display

        if self._widget_cache is None:
            self._widget_cache = {}
            display(self._get(default=widgets.VBox))

        elapsed_widget = self._get("elapsed", default=lambda: widgets.Label())
        elapsed_widget.value = "elapsed {}; updated {}".format(
            dt.timedelta(seconds=int(elapsed)),
            dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        )
        hbox_widget = self._get("title_hbox", default=widgets.HBox)
        hbox_widget.layout.align_items = "baseline"
        hbox_widget.children = [
            self._get("title", default=lambda: widgets.HTML("<h2>\u00FCberjob</h2>")),
            elapsed_widget,
        ]
        children = [hbox_widget]
        for section, title in (
            ("stale", "Determining stale value stores"),
            ("run", "Running graph"),
        ):
            scope_mapping = state.get(section)
            if scope_mapping:
                title_widget = self._get(
                    "section", section, "title", default=widgets.HTML
                )
                title_widget.value = "<b>{}</b>".format(html.escape(title))
                children.append(title_widget)
                for scope, scope_state in sorted_scope_items(scope_mapping):
                    progress_widget = self._get(
                        "section",
                        section,
                        "scope",
                        scope,
                        "progress",
                        default=widgets.IntProgress,
                    )
                    # Set progress bar's max before its value because the value's setter clamps it to the current max.
                    progress_widget.max = scope_state.total
                    progress_widget.value = scope_state.completed + scope_state.failed
                    bar_style = ""
                    if scope_state.completed == scope_state.total:
                        bar_style = "success"
                    elif scope_state.failed:
                        bar_style = "danger"
                    progress_widget.bar_style = bar_style
                    label_widget = self._get(
                        "section",
                        section,
                        "scope",
                        scope,
                        "label",
                        default=widgets.Label,
                    )
                    label_widget.value = "{}; {}".format(
                        scope_state.to_progress_string(),
                        get_scope_string(scope, add_zero_width_spaces=True),
                    )
                    hbox_widget = self._get(
                        "section", section, "scope", scope, "hbox", default=widgets.HBox
                    )
                    hbox_widget.children = [progress_widget, label_widget]
                    children.append(hbox_widget)

        if exception_tuples:
            title_widget = self._get("exception", default=widgets.HTML)
            title_widget.value = "<b>Exceptions</b>"
            children.append(title_widget)
            children.append(self._get_exception_accordion(exception_tuples))
        vbox_widget = self._get(default=widgets.VBox)
        vbox_widget.children = children

    def _get_exception_accordion(self, exception_tuples):
        import ipywidgets as widgets

        exception_accordion = self._get(
            "exception_accordion", default=widgets.Accordion
        )
        exception_text_widgets = []
        exception_titles = []
        for i, (scope, exception_tuple) in enumerate(exception_tuples):
            exception_text_widget = self._get("exception_text", i, default=widgets.HTML)
            exception_text_widget.value = (
                '<pre style="line-height: 120%">{}</pre>'.format(
                    html.escape("".join(traceback.format_exception(*exception_tuple)))
                )
            )
            exception_text_widgets.append(exception_text_widget)
            exception_titles.append(
                "Exception {}; {}".format(i + 1, get_scope_string(scope))
            )
        exception_accordion.children = exception_text_widgets
        for i, exception_title in enumerate(exception_titles):
            exception_accordion.set_title(i, exception_title)
        return exception_accordion

    def _output(self, value):
        pass
