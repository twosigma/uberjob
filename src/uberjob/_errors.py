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
from uberjob._util import fully_qualified_name
from uberjob._util.traceback import render_symbolic_traceback
from uberjob.graph import Call


class CallError(Exception):
    """
    An exception was raised in a symbolic call.

    :param call: The call.
    """

    def __init__(self, call: Call):
        super().__init__(
            "\n".join(
                [
                    f"An exception was raised in a symbolic call to {fully_qualified_name(call.fn)}.",
                    render_symbolic_traceback(call.stack_frame),
                ]
            )
        )
        self.call = call


class NotTransformedError(Exception):
    """An expected transformation was not applied."""


def create_chained_call_error(call: Call, exception: Exception) -> CallError:
    call_error = CallError(call)
    call_error.__cause__ = exception
    return call_error


__all__ = ["CallError", "NotTransformedError", "create_chained_call_error"]
