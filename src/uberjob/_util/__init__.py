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
from functools import lru_cache
from reprlib import Repr


def safe_max(*args):
    iterable = args[0] if len(args) == 1 else args
    return max((value for value in iterable if value is not None), default=None)


def is_ipython():
    try:
        __IPYTHON__
        return True
    except NameError:
        return False


compact_repr = Repr()
compact_repr.maxstring = 80
compact_repr.maxother = 80
compact_repr = compact_repr.repr


class OmittedType:
    __slots__ = ()

    def __repr__(self):
        return "<...>"


Omitted = OmittedType()


class MissingType:
    __slots__ = ()

    def __repr__(self):
        return "Missing"


Missing = MissingType()


class Slot:
    __slots__ = ("value",)

    def __init__(self, value=None):
        self.value = value


def repr_helper(instance, *args, defaults=None, **kwargs):
    defaults = defaults or {}
    args = list(args)
    while args:
        i = len(args) - 1
        if i not in defaults or defaults[i] != args[i]:
            break
        args.pop()

    return "{}({})".format(
        instance.__class__.__name__,
        ", ".join(
            [
                *map(compact_repr, args),
                *(
                    f"{k}={compact_repr(v)}"
                    for k, v in kwargs.items()
                    if defaults.get(k) != v
                ),
            ]
        ),
    )


@lru_cache(4096)
def fully_qualified_name(x):
    qualname = getattr(x, "__qualname__", None)
    if not qualname:
        if callable(x):
            return fully_qualified_name(x.__class__)
        return str(x)
    module = getattr(x, "__module__", None)
    if (
        not module
        or module.endswith("builtins")
        or module == "__main__"
        or module == "_operator"
    ):
        return qualname
    return f"{module}.{qualname}"
