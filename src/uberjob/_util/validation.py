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
import inspect
from collections.abc import Callable
from functools import lru_cache

from uberjob._util import fully_qualified_name


def assert_is_callable(argument_value, parameter_name, *, optional=False):
    """
    Asserts that an argument is callable.

    :param argument_value: The argument value to check.
    :param parameter_name: The name of the parameter.
    :param optional: True if the parameter is optional; ie it accepts None.
    """
    if optional and argument_value is None:
        return
    if not callable(argument_value):
        raise TypeError(
            f"The {parameter_name!r} parameter requires a callable but received {argument_value!r}."
        )


def assert_is_instance(
    argument_value, parameter_name, required_type, *, optional=False
):
    """
    Asserts that an argument is of the specified type.

    :param argument_value: The argument value to check.
    :param parameter_name: The name of the parameter.
    :param required_type: The required type.
    :param optional: True if the parameter is optional; ie it accepts None.
    """
    if optional and argument_value is None:
        return
    if not isinstance(argument_value, required_type):
        raise TypeError(
            f"The {parameter_name!r} parameter requires a {required_type!r}"
            f" but received a {type(argument_value)!r}."
        )


@lru_cache(4096)
def try_get_signature(fn: Callable):
    try:
        return inspect.signature(fn)
    except ValueError:
        return None


def assert_can_bind(fn: Callable, *args, **kwargs):
    sig = try_get_signature(fn)
    if sig is None:
        return
    try:
        sig.bind(*args, **kwargs)
    except TypeError as exception:
        raise TypeError(
            f"{fully_qualified_name(fn)} is not callable with the given arguments; {exception}"
        ) from None
