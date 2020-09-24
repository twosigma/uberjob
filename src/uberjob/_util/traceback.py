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
"""Provides symbolic traceback functionality."""
import inspect

from uberjob._util import Omitted, repr_helper


class StackFrame:
    """
    A stack frame for a call.

    :param name: The name of the code location.
    :param path: The path of the code location.
    :param line: The line number of the code location.
    :param outer: The outer stack frame.
    """

    __slots__ = ("name", "path", "line", "outer")

    def __init__(self, *, name, path, line, outer=None):
        self.name = name
        self.path = path
        self.line = line
        self.outer = outer

    def __repr__(self):
        return repr_helper(
            self,
            name=self.name,
            path=self.path,
            line=self.line,
            outer=Omitted if self.outer else None,
            defaults={"outer": None},
        )


class TruncatedStackFrameType:
    def __repr__(self):
        return "TruncatedStackFrame"


TruncatedStackFrame = TruncatedStackFrameType()


MAX_TRACEBACK_DEPTH = 15


def get_stack_frame(initial_depth=2):
    def recurse(frame, depth):
        if not frame:
            return None
        if depth < 0:
            return TruncatedStackFrame
        return StackFrame(
            name=frame.f_code.co_name,
            path=frame.f_code.co_filename,
            line=frame.f_lineno,
            outer=recurse(frame.f_back, depth - 1),
        )

    initial_frame = inspect.currentframe()
    while initial_depth:
        initial_frame = initial_frame.f_back
        initial_depth -= 1
    return recurse(initial_frame, MAX_TRACEBACK_DEPTH)


def render_symbolic_traceback(stack_frame):
    stack_frames = []
    while stack_frame:
        if stack_frame is TruncatedStackFrame:
            stack_frames.append(stack_frame)
            break
        if "/IPython/core/" in stack_frame.path:
            break
        stack_frames.append(stack_frame)
        stack_frame = stack_frame.outer

    def format_stack_frame(s):
        if s is TruncatedStackFrame:
            return "  ... truncated"
        return f'  File "{s.path}", line {s.line}, in {s.name}'

    return "\n".join(
        [
            "Symbolic traceback (most recent call last):",
            *(
                format_stack_frame(stack_frame)
                for stack_frame in reversed(stack_frames)
            ),
        ]
    )
