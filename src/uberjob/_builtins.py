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
import itertools

from uberjob._errors import NotTransformedError


def gather_list(*args):
    return list(args)


def gather_tuple(*args):
    return tuple(args)


def gather_set(*args):
    return set(args)


def gather_dict(*args):
    return dict(args)


def source():
    """
    A placeholder function used by registry.source.

    If this function is ever actually called, it means that a node was added to a plan via registry.source but then
    when uberjob.run was called the registry was not provided.

    :return:
    """
    raise NotTransformedError(
        "A source node was created via a Registry, but that Registry was not passed to uberjob.run."
    )


def unpack(iterable, length):
    t = tuple(itertools.islice(iterable, length + 1))
    if len(t) < length:
        raise ValueError(
            f"not enough values to unpack (expected {length}, got {len(t)})"
        )
    if len(t) > length:
        raise ValueError(f"too many values to unpack (expected {length})")
    return t
