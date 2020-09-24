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
from functools import wraps

from uberjob._util.validation import assert_is_instance


def identity(x):
    return x


def create_retry(attempts, exc_type=Exception):
    """Decorator for retrying a function call"""
    assert_is_instance(attempts, "attempts", int)
    if attempts < 1:
        raise ValueError("attempts must be positive.")
    if attempts == 1:
        return identity

    def inner_retry(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            for attempt_index in range(attempts):
                try:
                    return f(*args, **kwargs)
                except exc_type:
                    is_last_attempt = attempt_index == attempts - 1
                    if is_last_attempt:
                        raise

        return wrapper

    return inner_retry
