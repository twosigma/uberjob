AsyncValueStore Support
======================

Starting with version 1.1.0, uberjob supports asynchronous value stores, providing better performance for I/O-bound operations.

AsyncValueStore Interface
------------------------

The :class:`~uberjob.AsyncValueStore` abstract base class defines the interface for all asynchronous value stores:

.. code-block:: python

   class AsyncValueStore(ABC):
       @abstractmethod
       async def read(self):
           """Read the value from the store asynchronously."""

       @abstractmethod
       async def write(self, value) -> None:
           """Write a value to the store asynchronously."""

       @abstractmethod
       async def get_modified_time(self) -> dt.datetime | None:
           """Get the modified time of the stored value asynchronously."""

Available Implementations
------------------------

uberjob provides several implementations of :class:`~uberjob.AsyncValueStore`:

- :class:`~uberjob.stores.AsyncJsonFileStore`: For storing JSON-serializable values in files asynchronously
- :class:`~uberjob.stores.AsyncTextFileStore`: For storing string values in files asynchronously
- :class:`~uberjob.stores.AsyncBinaryFileStore`: For storing binary data in files asynchronously
- :class:`~uberjob.stores.AsyncPickleFileStore`: For storing Python objects in files asynchronously
- :class:`~uberjob.stores.AsyncTouchFileStore`: For creating empty marker files asynchronously

Example Usage
-----------

Here's an example of using :class:`~uberjob.stores.AsyncJsonFileStore`:

.. code-block:: python

   import asyncio
   from uberjob.stores import AsyncJsonFileStore

   async def main():
       # Create an async JSON file store
       store = AsyncJsonFileStore("data.json")
       
       # Write data asynchronously
       await store.write({"key": "value"})
       
       # Read data asynchronously
       data = await store.read()
       print(data)  # {"key": "value"}
       
       # Get the modified time
       modified_time = await store.get_modified_time()
       print(f"Last modified: {modified_time}")

   asyncio.run(main())

Utility Functions
---------------

uberjob also provides async versions of utility functions for file operations:

- :func:`~uberjob.stores.async_staged_write`: Asynchronously write to a staged file that is atomically moved to the target path
- :func:`~uberjob.stores.async_staged_write_path`: Lower-level function that yields a staging path
- :func:`~uberjob.stores.async_get_modified_time`: Get the modified time of a file asynchronously

Performance Considerations
------------------------

Asynchronous value stores are most beneficial in scenarios where:

- Many I/O operations are performed concurrently
- I/O operations take significant time (network operations, large files)
- The application already uses asyncio for other purposes

For small, local files or CPU-bound workloads, the synchronous value stores may still be more appropriate.
