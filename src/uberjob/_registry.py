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
import copy
import typing

from uberjob._builtins import source
from uberjob._plan import Node, Plan
from uberjob._util import validation
from uberjob._util.traceback import get_stack_frame
from uberjob._value_store import ValueStore


class RegistryValue:
    __slots__ = ("value_store", "is_source", "stack_frame")

    def __init__(self, value_store, *, is_source, stack_frame):
        self.value_store = value_store
        self.is_source = is_source
        self.stack_frame = stack_frame


class Registry:
    """A mapping from :class:`~uberjob.graph.Node` to :class:`~uberjob.ValueStore`."""

    def __init__(self):
        self.mapping = {}

    def add(self, node: Node, value_store: ValueStore) -> None:
        """
        Assign a :class:`~uberjob.graph.Node` to a :class:`~uberjob.ValueStore`.

        :param node: The plan node.
        :param value_store: The value store for the node.
        """
        validation.assert_is_instance(node, "node", Node)
        validation.assert_is_instance(value_store, "value_store", ValueStore)
        if node in self.mapping:
            raise Exception("The node already has a value store.")
        self.mapping[node] = RegistryValue(
            value_store, is_source=False, stack_frame=get_stack_frame()
        )

    def source(self, plan: Plan, value_store: ValueStore) -> Node:
        """
        Create a :class:`~uberjob.graph.Node` in the :class:`~uberjob.Plan` that reads from the
        given :class:`~uberjob.ValueStore`.

        :param plan: The plan to add a source node to.
        :param value_store: The value store to read from.
        :return: The newly added plan node.
        """
        validation.assert_is_instance(plan, "plan", Plan)
        validation.assert_is_instance(value_store, "value_store", ValueStore)
        stack_frame = get_stack_frame()
        node = plan.call(source)
        node.stack_frame = stack_frame
        self.mapping[node] = RegistryValue(
            value_store, is_source=True, stack_frame=stack_frame
        )
        return node

    def __contains__(self, node: Node) -> bool:
        """
        Check if the :class:`~uberjob.graph.Node` has a :class:`~uberjob.ValueStore`.

        :param node: The plan node.
        :return: True if the node has a value store.
        """
        return node in self.mapping

    def __getitem__(self, node: Node) -> ValueStore:
        """
        Get the :class:`~uberjob.ValueStore` for a :class:`~uberjob.graph.Node`.

        :param node: The plan node.
        :return: The value store for the node.
        """
        return self.mapping[node].value_store

    def get(self, node: Node) -> typing.Optional[ValueStore]:
        """
        Get the :class:`~uberjob.ValueStore` for a :class:`~uberjob.graph.Node` if it has one, or ``None``.

        :param node: The plan node.
        :return: The value store for the node, or ``None``.
        """
        v = self.mapping.get(node)
        return v.value_store if v else None

    def keys(self) -> typing.Iterable[Node]:
        """
        Get all registered :class:`~uberjob.graph.Node` instances.

        :return: An iterable of :class:`~uberjob.graph.Node`.
        """
        return self.mapping.keys()

    def values(self) -> typing.Iterable[ValueStore]:
        """
        Get all registered :class:`~uberjob.ValueStore` instances.

        :return:  An iterable of :class:`~uberjob.ValueStore`.
        """
        return [v.value_store for v in self.mapping.values()]

    def items(self) -> typing.Iterable[typing.Tuple[Node, ValueStore]]:
        """
        Get all registered (node, value_store) pairs.

        :return: An iterable of (node, value_store) pairs.
        """
        return [(k, v.value_store) for k, v in self.mapping.items()]

    def __iter__(self) -> typing.Iterable[Node]:
        """
        Get all registered :class:`~uberjob.graph.Node` instances.

        :return: An iterable of :class:`~uberjob.graph.Node`.
        """
        return iter(self.mapping)

    def __len__(self) -> int:
        """
        Get the number of registered (node, value_store) pairs.

        :return: The number of (node, value_store) pairs.
        """
        return len(self.mapping)

    def copy(self):
        """
        Make a copy of this :class:`~uberjob.Registry`.

        :return: The new copy.
        """
        new_registry = Registry()
        new_registry.mapping = {
            node: copy.copy(registry_value)
            for node, registry_value in self.mapping.items()
        }
        return new_registry
