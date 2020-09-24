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
import datetime as dt
import itertools
import os
import pathlib
from functools import partial
from tempfile import TemporaryDirectory
from unittest import TestCase

from uberjob._testing import TestMountedFileStore
from uberjob.stores import (
    BinaryFileStore,
    FileStore,
    JsonFileStore,
    ModifiedTimeSource,
    MountedStore,
    PathSource,
    PickleFileStore,
    TextFileStore,
    TouchFileStore,
    staged_write,
    staged_write_path,
)
from uberjob.stores._file_store import STAGING_SUFFIX


class ValueStoresTestCase(TestCase):
    def test_round_trip(self):
        create_file_stores = [
            (JsonFileStore, {"encoding": "utf-8"}),
            (PickleFileStore, {}),
            (TextFileStore, {"encoding": "utf-8"}),
        ]
        values = [
            "hello world!",
            3,
            7.0,
            False,
            r"utf8: ¯\_(ツ)_/¯, ❨╯°□°❩╯︵┻━┻",
            None,
            {"hello": True, "world": 3.0},
            {"nested": ["objects", {r"¯\_(ツ)_/¯": "with"}], "utf8": True},
        ]
        for (create_file_store, kwargs), value in itertools.product(
            create_file_stores, values
        ):
            if create_file_store is TextFileStore and not isinstance(value, str):
                continue
            with self.subTest(create_file_store=create_file_store, value=value):
                value_store = TestMountedFileStore(partial(create_file_store, **kwargs))
                value_store.write(value)
                self.assertEqual(value_store.read(), value)
                repr(value_store)
                MountedStore.__repr__(value_store)
                value_store.get_modified_time()

    def test_round_trip_binary(self):
        value_store = TestMountedFileStore(BinaryFileStore)
        value = b"hello world!"
        value_store.write(value)
        self.assertEqual(value_store.read(), value)

    def test_pathlib_path(self):
        with TemporaryDirectory() as tempdir:
            s = "hello world"
            value_store = TextFileStore(pathlib.Path(tempdir) / "myfile")
            self.assertIsNone(value_store.get_modified_time())
            value_store.write(s)
            self.assertIsNotNone(value_store.get_modified_time())
            self.assertEqual(value_store.read(), s)

    def test_staged_write(self):
        with TemporaryDirectory() as tempdir:
            s1 = "hello world"
            p = os.path.join(tempdir, "myfile")
            staging_p = f"{p}{STAGING_SUFFIX}"

            with staged_write(p) as outputfile:
                outputfile.write(s1)
            self.assertEqual(TextFileStore(p).read(), s1)

            with self.assertRaises(ValueError):
                with staged_write(p, "r") as _:
                    pass
            self.assertEqual(TextFileStore(p).read(), s1)

            s2 = "fizz buzz"
            self.assertFalse(os.path.exists(staging_p))
            with self.assertRaises(ValueError):
                with staged_write(p) as outputfile:
                    outputfile.write(s2)
                    raise ValueError()
            self.assertEqual(TextFileStore(p).read(), s1)
            self.assertFalse(os.path.exists(staging_p))

            with staged_write(p) as outputfile:
                outputfile.write(s2)
            self.assertEqual(TextFileStore(p).read(), s2)

    def _staged_write_path_helper(self, tempdir, use_pathlib_path):
        s1 = "hello world"
        p = os.path.join(tempdir, "myfile")
        if use_pathlib_path:
            p = pathlib.Path(p)

        with self.assertRaises(OSError):
            with staged_write_path(p) as _:
                pass

        with staged_write_path(p) as staging_p:
            self.assertEqual(type(p), type(staging_p))
            with open(staging_p, "w") as outputfile:
                outputfile.write(s1)
        self.assertEqual(TextFileStore(p).read(), s1)

        staging_p2 = f"{p}{STAGING_SUFFIX}"

        for should_write in (True, False):
            with self.subTest(should_write=should_write):
                with self.assertRaises(ValueError):
                    with staged_write_path(p) as staging_p:
                        if should_write:
                            with open(staging_p, "w") as _:
                                pass
                        raise ValueError()
                self.assertFalse(os.path.exists(staging_p2))

    def test_staged_write_path(self):
        for use_pathlib_path in (True, False):
            with self.subTest(use_pathlib_path=use_pathlib_path):
                with TemporaryDirectory() as tempdir:
                    self._staged_write_path_helper(tempdir, use_pathlib_path)

    def test_touch_file_store(self):
        with TemporaryDirectory() as tempdir:
            p = os.path.join(tempdir, "myfile")
            value_store = TouchFileStore(p)
            self.assertIsNone(value_store.get_modified_time())
            FileStore.__repr__(value_store)
            value_store.write(None)
            with self.assertRaises(TypeError):
                value_store.write(7)
            self.assertIsNone(value_store.read())
            self.assertIsNotNone(value_store.get_modified_time())

            TextFileStore(p).write("hello world")
            with self.assertRaises(IOError):
                value_store.read()

    def test_path_source(self):
        with TemporaryDirectory() as tempdir:
            p = os.path.join(tempdir, "myfile")

            value_store = PathSource(p)
            with self.assertRaises(NotImplementedError):
                value_store.write("hello world")
            with self.assertRaises(IOError):
                value_store.get_modified_time()
            self.assertEqual(value_store.read(), p)

            value_store = PathSource(p, required=False)
            self.assertIsNone(value_store.get_modified_time())
            with self.assertRaises(IOError):
                value_store.read()

            repr(value_store)

    def test_modified_time_source(self):
        t = None
        value_store = ModifiedTimeSource(None)
        with self.assertRaises(NotImplementedError):
            value_store.write(7)
        self.assertIsNone(value_store.read())
        self.assertIsNone(value_store.get_modified_time())
        with self.assertRaises(TypeError):
            ModifiedTimeSource(dt.date(2019, 1, 2))
        t = dt.datetime(2019, 1, 2, 3, 4, 5)
        value_store = ModifiedTimeSource(t)
        self.assertEqual(value_store.read(), t)
        self.assertEqual(value_store.get_modified_time(), t)
        repr(value_store)
