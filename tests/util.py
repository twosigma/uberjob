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
import re
from contextlib import contextmanager
from unittest import TestCase

from uberjob._errors import CallError
from uberjob._util.traceback import StackFrame


def _traceback_summary(traceback) -> list[str]:
    result = []
    while traceback is not None:
        result.append(traceback.tb_frame.f_code.co_name)
        traceback = traceback.tb_next
    return result


def _exception_chain_traceback_summary(exception) -> list[list[str]]:
    result = []
    while exception is not None:
        result.append(_traceback_summary(exception.__traceback__))
        exception = exception.__cause__
    return result


@contextmanager
def assert_call_exception(
    test_case,
    expected_exception=None,
    expected_regex=None,
    expected_stack_frame=None,
    expected_exception_chain_traceback_summary=None,
):
    try:
        yield
    except CallError as e:
        test_case.assertRegex(str(e), "An exception was raised in a symbolic call.*")
        if expected_exception is not None:
            test_case.assertIsInstance(e.__cause__, expected_exception)
        if expected_regex is not None:
            test_case.assertRegex(str(e.__cause__), expected_regex)
        if expected_stack_frame is not None:
            stack_frame = e.call.stack_frame
            test_case.assertEqual(stack_frame.name, expected_stack_frame.name)
            test_case.assertEqual(stack_frame.path, expected_stack_frame.path)
            test_case.assertEqual(stack_frame.line, expected_stack_frame.line)
        if expected_exception_chain_traceback_summary is not None:
            test_case.assertEqual(
                _exception_chain_traceback_summary(e.__cause__),
                expected_exception_chain_traceback_summary,
            )
    else:
        test_case.fail("Call exception not raised.")


def copy_with_line_offset(stack_frame, line_offset):
    return StackFrame(
        name=stack_frame.name,
        path=stack_frame.path,
        line=stack_frame.line + line_offset,
        outer=stack_frame.outer,
    )


class UberjobTestCase(TestCase):
    assert_call_exception = assert_call_exception

    @contextmanager
    def assert_forgotten_registry(self):
        expected_regex = re.escape(
            "A source node was created via a Registry, but that Registry was not passed to uberjob.run."
        )
        with self.assert_call_exception(Exception, expected_regex):
            yield

    @contextmanager
    def assert_failed_to_bind(self):
        with self.assertRaisesRegex(
            TypeError, re.escape(" is not callable with the given arguments; ")
        ):
            yield

    @contextmanager
    def assert_failed_to_read_from_empty_store(self):
        expected_regex = re.escape("Failed to read value from empty store.")
        with self.assert_call_exception(Exception, expected_regex):
            yield
