#
# Copyright 2025 Two Sigma Open Source, LLC
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
import aiofiles
import datetime as dt
import itertools
import os
import pathlib

from tempfile import TemporaryDirectory

import pytest

from uberjob.stores import (
    AsyncBinaryFileStore,
    AsyncJsonFileStore,
    AsyncPickleFileStore,
    AsyncTextFileStore,
    AsyncTouchFileStore,
    async_staged_write,
    async_staged_write_path,
)
from uberjob.stores._file_store import STAGING_SUFFIX


@pytest.mark.asyncio
async def test_async_round_trip():
    """Test that each async file store can write and read back the same values."""
    create_file_stores = [
        (AsyncJsonFileStore, {"encoding": "utf-8"}),
        (AsyncPickleFileStore, {}),
        (AsyncTextFileStore, {"encoding": "utf-8"}),
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
        # Skip text file store for non-string values
        if create_file_store is AsyncTextFileStore and not isinstance(value, str):
            continue

        with TemporaryDirectory() as tempdir:
            path = os.path.join(tempdir, "test_file")
            value_store = create_file_store(path, **kwargs)

            # Test write and read
            await value_store.write(value)
            read_value = await value_store.read()
            assert read_value == value

            # Test modified time
            modified_time = await value_store.get_modified_time()
            assert isinstance(modified_time, dt.datetime)


@pytest.mark.asyncio
async def test_async_binary_file_store():
    """Test AsyncBinaryFileStore specifically."""
    with TemporaryDirectory() as tempdir:
        path = os.path.join(tempdir, "test_binary")
        value_store = AsyncBinaryFileStore(path)

        # Test write and read
        value = b"hello world!"
        await value_store.write(value)
        read_value = await value_store.read()
        assert read_value == value


@pytest.mark.asyncio
async def test_pathlib_path():
    """Test using pathlib.Path with AsyncTextFileStore."""
    with TemporaryDirectory() as tempdir:
        s = "hello world"
        path = pathlib.Path(tempdir) / "myfile"
        value_store = AsyncTextFileStore(path)

        # Initial modified time should be None
        assert await value_store.get_modified_time() is None

        # After writing, modified time should be set
        await value_store.write(s)
        assert await value_store.get_modified_time() is not None

        # Value should be readable
        assert await value_store.read() == s


@pytest.mark.asyncio
async def test_async_staged_write():
    """Test async_staged_write utility."""
    with TemporaryDirectory() as tempdir:
        s1 = "hello world"
        p = os.path.join(tempdir, "myfile")
        staging_p = f"{p}{STAGING_SUFFIX}"

        # Test successful write
        async with async_staged_write(p) as outputfile:
            await outputfile.write(s1)
        assert await AsyncTextFileStore(p).read() == s1

        # Test invalid mode
        with pytest.raises(ValueError):
            async with async_staged_write(p, "r") as _:
                pass
        assert await AsyncTextFileStore(p).read() == s1

        # Test exception during write
        s2 = "fizz buzz"
        assert not os.path.exists(staging_p)

        with pytest.raises(ValueError):
            async with async_staged_write(p) as outputfile:
                await outputfile.write(s2)
                raise ValueError()

        # Original file should remain unchanged
        assert await AsyncTextFileStore(p).read() == s1
        assert not os.path.exists(staging_p)

        # Successful write should update the file
        async with async_staged_write(p) as outputfile:
            await outputfile.write(s2)
        assert await AsyncTextFileStore(p).read() == s2


@pytest.mark.asyncio
async def test_async_staged_write_path():
    """Test async_staged_write_path utility."""

    async def _staged_write_path_helper(tempdir, use_pathlib_path):
        s1 = "hello world"
        p = os.path.join(tempdir, "myfile")
        if use_pathlib_path:
            p = pathlib.Path(p)

        # No file should exist at first
        staging_p2 = f"{p}{STAGING_SUFFIX}"

        # Create our file with staged write
        async with async_staged_write_path(p) as staging_p:
            assert type(p) == type(staging_p)
            async with aiofiles.open(staging_p, "w") as outputfile:
                await outputfile.write(s1)
        assert await AsyncTextFileStore(p).read() == s1

        # Test exceptions with and without creating the file
        for should_write in (True, False):
            with pytest.raises(ValueError):
                async with async_staged_write_path(p) as staging_p:
                    if should_write:
                        async with aiofiles.open(staging_p, "w") as _:
                            pass
                    raise ValueError()
            assert not os.path.exists(staging_p2)

    with TemporaryDirectory() as tempdir:
        await _staged_write_path_helper(tempdir, False)

    with TemporaryDirectory() as tempdir:
        await _staged_write_path_helper(tempdir, True)


@pytest.mark.asyncio
async def test_async_touch_file_store():
    """Test AsyncTouchFileStore functionality."""
    with TemporaryDirectory() as tempdir:
        p = os.path.join(tempdir, "myfile")
        value_store = AsyncTouchFileStore(p)

        # Initially, no file exists
        assert await value_store.get_modified_time() is None

        # Write None to create the touch file
        await value_store.write(None)

        # Verify we can't write any other value
        with pytest.raises(TypeError):
            await value_store.write(7)

        # Verify we can read None back
        assert await value_store.read() is None

        # Verify the file now has a modified time
        assert await value_store.get_modified_time() is not None

        # Write content to the file to make it non-empty
        with open(p, "w") as f:
            f.write("hello world")

        # AsyncTouchFileStore.read should raise an error for non-empty files
        with pytest.raises(OSError):
            await value_store.read()
