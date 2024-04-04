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
import operator
from collections.abc import Callable, Generator
from contextlib import contextmanager
from threading import RLock

from uberjob import _builtins
from uberjob._util import validation
from uberjob._util.traceback import get_stack_frame
from uberjob.graph import (
    Call,
    Dependency,
    Graph,
    KeywordArg,
    Literal,
    Node,
    PositionalArg,
)

# These are restricted to Python's general purpose built-in containers.
GATHER_LOOKUP = {
    list: _builtins.gather_list,
    tuple: _builtins.gather_tuple,
    set: _builtins.gather_set,
    dict: _builtins.gather_dict,
}


def _is_node(value) -> bool:
    """Efficiently determines whether the given value is a :class:`~uberjob.graph.Node`."""
    return type(value) in (Call, Literal)


class Plan:
    """Represents a symbolic call graph."""

    def __init__(self):
        self.graph = Graph()
        """The underlying :class:`networkx.MultiDiGraph`."""
        self._scope = ()
        self._scope_lock = RLock()

    def _call(self, stack_frame, fn: Callable, *args, **kwargs) -> Call:
        call = Call(fn, scope=self._scope, stack_frame=stack_frame)
        self.graph.add_node(call)
        for index, arg in enumerate(args):
            self.graph.add_edge(
                self._gather(stack_frame, arg), call, PositionalArg(index)
            )
        for index, (name, arg) in enumerate(kwargs.items()):
            self.graph.add_edge(
                self._gather(stack_frame, arg), call, KeywordArg(name, index)
            )
        return call

    def call(self, fn: Callable, /, *args, **kwargs) -> Call:
        """
        Add a function call to this :class:`~uberjob.Plan`.

        Non-symbolic arguments are automatically converted to symbolic arguments using :func:`~uberjob.Plan.gather`.

        :param fn: The function to be called.
        :param args: The symbolic positional arguments.
        :param kwargs: The symbolic keyword arguments.
        :return: The symbolic result of the function call.
        :raises TypeError: If arguments fail to bind to parameters.
        """
        validation.assert_is_callable(fn, "fn")
        validation.assert_can_bind(fn, *args, **kwargs)
        return self._call(get_stack_frame(), fn, *args, **kwargs)

    def lit(self, value) -> Literal:
        """
        Add a literal value to this :class:`~uberjob.Plan`.

        :param value: The literal value.
        :return: The symbolic literal value.
        """
        if _is_node(value):
            raise TypeError(f"The value is already a {Node.__name__}.")
        literal = Literal(value, scope=self._scope)
        self.graph.add_node(literal)
        return literal

    def add_dependency(self, source: Node, target: Node) -> None:
        """
        Add a dependency indicating that the source :class:`~uberjob.graph.Node` must run before the
        target :class:`~uberjob.graph.Node`.

        :param source: The :class:`~uberjob.graph.Node` that is depended on.
        :param target: The dependent :class:`~uberjob.graph.Node`.
        """
        validation.assert_is_instance(source, "source", Node)
        validation.assert_is_instance(target, "target", Node)
        for node in (source, target):
            if not self.graph.has_node(node):
                raise KeyError(f"The plan graph does not contain the node {node!r}.")
        self.graph.add_edge(source, target, Dependency())

    def _gather(self, stack_frame, value) -> Node:
        def recurse(root):
            root_type = type(root)
            gather_fn = GATHER_LOOKUP.get(root_type)
            if gather_fn is not None:
                items = root.items() if root_type is dict else root
                children = [recurse(item) for item in items]
                if any(_is_node(child) for child in children):
                    return self._call(stack_frame, gather_fn, *children)
            return root

        value = recurse(value)
        return value if _is_node(value) else self.lit(value)

    def gather(self, value) -> Node:
        """
        Gather a structured value that may contain instances of :class:`~uberjob.graph.Node` into a single
        :class:`~uberjob.graph.Node` representing the entire structured value.

        If the value is already a :class:`~uberjob.graph.Node`, it will be returned unchanged.

        When navigating the structured value, gather will only recognize Python's general purpose built-in containers:
        :class:`dict`, :class:`list`, :class:`set`, and :class:`tuple`.

        :param value: A structured value that may contain instances of :class:`~uberjob.graph.Node`.
        :return: A single :class:`~uberjob.graph.Node` representing the gathered input value.
        """
        return self._gather(get_stack_frame(), value)

    def unpack(self, iterable, length: int) -> tuple[Node, ...]:
        """
        Unpack a symbolic iterable into a tuple of symbolic values.

        :param iterable: The symbolic iterable.
        :param length: The number of values in the iterable.
        :return: A tuple of :class:`~uberjob.graph.Node`.
        """
        if not isinstance(length, int) or length < 0:
            raise ValueError("length must be a non-negative integer.")
        stack_frame = get_stack_frame()
        t = self._call(stack_frame, _builtins.unpack, iterable, length)
        return tuple(
            self._call(stack_frame, operator.getitem, t, index)
            for index in range(length)
        )

    @contextmanager
    def scope(self, *args) -> Generator[None, None, None]:
        """
        A context manager for organizing a :class:`~uberjob.Plan`.

        :param args: Values to append to the end of the current scope; they must be hashable and equatable.
        """
        with self._scope_lock:
            parent_scope = self._scope
            child_scope = parent_scope + args
            self._scope = child_scope
            try:
                yield
            finally:
                if self._scope != child_scope:
                    raise Exception(
                        "Plan scopes must be entered and exited in stack order."
                    )
                self._scope = parent_scope

    def copy(self) -> "Plan":
        """
        Make a copy of this :class:`~uberjob.Plan`.

        The new copy starts with an empty scope.

        :return: The new copy.
        """
        new_plan = Plan()
        new_plan.graph = self.graph.copy()
        return new_plan

    __copy__ = copy
