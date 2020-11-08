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
from abc import ABC, abstractmethod
from typing import Tuple


class ProgressObserver(ABC):
    """The abstract base class for all progress observers."""

    @abstractmethod
    def __enter__(self):
        """Start observing progress."""

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop observing progress."""

    @abstractmethod
    def increment_total(self, *, section: str, scope: Tuple, amount: int):
        """
        Increment the number of entries in this section and scope by the specified amount.

        :param section: The section.
        :param scope: The scope.
        :param amount: The amount.
        """

    @abstractmethod
    def increment_running(self, *, section: str, scope: Tuple):
        """
        Increment the number of running entries in this section and scope. This method must be thread-safe.

        :param section: The section.
        :param scope: The scope.
        """

    @abstractmethod
    def increment_completed(self, *, section: str, scope: Tuple):
        """
        Increment the number of completed entries in this section and scope. This method must be thread-safe.

        :param section: The section.
        :param scope: The scope.
        """

    @abstractmethod
    def increment_failed(self, *, section: str, scope: Tuple, exception: Exception):
        """
        Increment the number of failed entries in this section and scope. This method must be thread-safe.

        :param section: The section.
        :param scope: The scope.
        :param exception: The exception.
        """
