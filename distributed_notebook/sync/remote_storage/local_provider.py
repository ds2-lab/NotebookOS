import asyncio
import sys
import threading
import time
from typing import Any, Dict, Optional

from distributed_notebook.sync.remote_storage.error import InvalidKeyError
from distributed_notebook.sync.remote_storage.remote_storage_provider import RemoteStorageProvider


class LocalStorageProvider(RemoteStorageProvider):
    """
    LocalStorageProvider extends RemoteStorageProvider but simply reads and writes data from and to a dictionary.

    It is intended only to be used while unit testing, such as with the LocalCheckpointer class.
    """
    def __init__(
            self,
            maximum_state_size_bytes: int = -1,
            **kwargs,
    ):
        """
        :param maximum_state_size_bytes: enables setting a maximum size (in bytes) for state that is being
                                         checkpointed. set to value < 0 for no limit.
        """
        super().__init__()

        self._data: Dict[str, Any] = dict()
        self._async_lock: asyncio.Lock = asyncio.Lock()
        self._lock: threading.Lock = threading.Lock()
        self._maximum_state_size_bytes: int = maximum_state_size_bytes

    @property
    def storage_name(self) -> str:
        return f"Local In-Memory Storage"

    @property
    def size(self) -> int:
        return len(self._data)

    @property
    def maximum_state_size_bytes(self)->int:
        return self._maximum_state_size_bytes

    def __len__(self) -> int:
        return self.size

    def is_too_large(self, size_bytes: int)->bool:
        """
        :param size_bytes: the size of the data to (potentially) be written to remote remote_storage
        :return: True if the data is too large to be written, otherwise False
        """
        return False # never too large!

    async def write_value_async(self, key: str, value: Any)->bool:
        """
        Asynchronously write a value to Local In-Memory Storage at the specified key.

        :param key: the key at which to store the value in Local In-Memory Storage.
        :param value: the value to be written.
        """
        async with self._async_lock:
            value_size: int = sys.getsizeof(value)

            start_time: float = time.time()

            self._data[key] = value

            end_time: float = time.time()
            time_elapsed: float = end_time - start_time
            time_elapsed_ms: float = round(time_elapsed * 1.0e3)

        # Update internal metrics.
        self.update_write_stats(
            time_elapsed_ms=time_elapsed,
            size_bytes=value_size,
            num_values=1
        )

        self.log.debug(f'Wrote value of size {value_size} bytes to {self.storage_name} at key "{key}" in {time_elapsed_ms:,} ms.')

        return True

    def write_value(self, key: str, value: Any)->bool:
        """
        Write a value to Local In-Memory Storage at the specified key.

        :param key: the key at which to store the value in Local In-Memory Storage.
        :param value: the value to be written.
        """
        value_size: int = sys.getsizeof(value)

        start_time: float = time.time()

        self._data[key] = value

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)

        # Update internal metrics.
        self.update_write_stats(
            time_elapsed_ms=time_elapsed,
            size_bytes=value_size,
            num_values=1
        )

        self.log.debug(f'Wrote value of size {value_size} bytes to {self.storage_name} '
                       f'at key "{key}" in {time_elapsed_ms:,} ms.')

        return True

    async def read_value_async(self, key: str) -> Any:
        """
        Asynchronously read a value from Local In-Memory Storage.
        :param key: the Local In-Memory Storage key from which to read the value.

        :return: the value read from Local In-Memory Storage.
        """
        async with self._async_lock:
            return self.read_value(key)

    def read_value(self, key: str) -> Any:
        """
        Read a value from Local In-Memory Storage from the specified key.
        :param key: the Local In-Memory Storage key from which to read the value.

        :return: the value read from Local In-Memory Storage.
        """
        start_time: float = time.time()

        value: Optional[str | bytes | memoryview] = self._data.get(key, None)

        if value is None:
            raise InvalidKeyError(f'No data stored in LocalStorageProvider at key "{key}"', key = key)

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)
        value_size = sys.getsizeof(value)

        # Update internal metrics.
        self.update_read_stats(
            time_elapsed_ms=time_elapsed,
            size_bytes=value_size,
            num_values=1
        )

        self.log.debug(f'Read value of size {value_size} bytes from {self.storage_name} from key "{key}" '
                       f'in {time_elapsed_ms:,} ms.')

        return value

    async def delete_value_async(self, key: str)->bool:
        """
        Asynchronously delete the value stored at the specified key from Local In-Memory Storage.

        :param key: the name/key of the data to delete
        """
        async with self._async_lock:
            start_time: float = time.time()

            if key in self._data:
                del self._data[key]

            end_time: float = time.time()
            time_elapsed: float = end_time - start_time
            time_elapsed_ms: float = round(time_elapsed * 1.0e3)

            self.update_delete_stats(time_elapsed, 1)

            self.log.debug(f'Deleted value stored at key "{key}" from {self.storage_name} in {time_elapsed_ms:,} ms.')

        return True

    def delete_value(self, key: str)->bool:
        """
        Delete the value stored at the specified key from Local In-Memory Storage.

        :param key: the name/key of the data to delete
        """
        start_time: float = time.time()

        if key in self._data:
            del self._data[key]

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)

        self.update_delete_stats(time_elapsed, 1)

        self.log.debug(f'Deleted value stored at key "{key}" from {self.storage_name} in {time_elapsed_ms:,} ms.')

        return True

    async def close_async(self):
        self.close()

    def close(self):
        self._data.clear()

    get = read_value
    set = write_value