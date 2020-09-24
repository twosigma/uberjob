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
"""The state of a running Plan can be observed through the use of Progress."""
import pathlib
from functools import partial
from typing import Callable, Union

from uberjob._util import is_ipython

from ._composite_progress_observer import CompositeProgressObserver
from ._console_progress_observer import ConsoleProgressObserver
from ._html_progress_observer import HtmlProgressObserver
from ._ipython_progress_observer import IPythonProgressObserver
from ._null_progress_observer import NullProgressObserver
from ._progress import Progress
from ._progress_observer import ProgressObserver

null_progress = Progress(NullProgressObserver)
"""Ignore observed progress."""


console_progress = Progress(
    partial(ConsoleProgressObserver, min_update_interval=30, max_update_interval=300)
)
"""Display observed progress using the console."""


ipython_progress = Progress(
    partial(IPythonProgressObserver, min_update_interval=1, max_update_interval=10)
)
"""Display observed progress using IPython widgets."""


def html_progress(
    output: Union[str, pathlib.Path, Callable[[bytes], None]]
) -> Progress:
    """Write observed progress to an HTML file."""
    return Progress(
        partial(
            HtmlProgressObserver, output, min_update_interval=30, max_update_interval=60
        )
    )


default_progress = ipython_progress if is_ipython() else console_progress
"""The default method for observing progress."""


def composite_progress(*members: Progress) -> Progress:
    """Create a progress observer that forwards observations to all of its members."""

    def make_observer():
        return CompositeProgressObserver(progress.observer() for progress in members)

    return Progress(make_observer)


__all__ = [
    "Progress",
    "ProgressObserver",
    "null_progress",
    "console_progress",
    "ipython_progress",
    "html_progress",
    "default_progress",
    "composite_progress",
]
