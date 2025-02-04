import sys
import time

from distributed_notebook.sync.storage.storage_provider import RemoteStorageProvider

import asyncio
import redis
import redis.asyncio as async_redis

from typing import Any, Optional

class RedisProvider(RemoteStorageProvider):
    def __init__(
            self,
            host:str = "",
            port: int = 6379,
            db: int = 0,
            password: Optional[str] = None,
            additional_redis_args: Optional[dict] = None
    ):
        super().__init__()

        if ':' in host:
            host_orig:str = host
            idx = host_orig.index(':')
            host = host_orig[:idx]

            try:
                port = int(host_orig[idx+1:]) # +1 because idx is the index of the ':', not the beginning of the port
            except IndexError:
                # Apparently there's nothing after the colon, so we'll use the default of 6379 (or whatever was passed).
                pass

        self._redis_password: str = password
        self._redis_db: int = db
        self._redis_port = port
        self._redis_host = host

        if additional_redis_args is None:
            additional_redis_args = dict()

        self._additional_redis_args: dict[str, Any] = additional_redis_args
        self._lock = asyncio.Lock()

        self.log.debug(f"Connecting to Redis server at {host}:{port} (db={db}).")
        self._async_redis = async_redis.Redis(host = host, port = port, db = db, password = password, **additional_redis_args)
        self._redis = redis.Redis(host = host, port = port, db = db, password = password, **additional_redis_args)

    @property
    def storage_name(self) -> str:
        return "Redis"

    async def write_value_async(self, key: str, value: Any):
        """
        Asynchronously write a value to Redis at the specified key.

        :param key: the key at which to store the value in Redis.
        :param value: the value to be written.
        """
        value_size: int = sys.getsizeof(value)

        start_time: float = time.time()

        await self._async_redis.set(key, value)

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)

        self._write_time += time_elapsed
        self._num_objects_written += 1
        self._bytes_written += value_size

        self.log.debug(f'Wrote value of size {value_size} bytes to Redis at key "{key}" in {time_elapsed_ms:,} ms.')

    def write_value(self, key: str, value: Any):
        """
        Write a value to Redis at the specified key.

        :param key: the key at which to store the value in Redis.
        :param value: the value to be written.
        """
        value_size: int = sys.getsizeof(value)

        start_time: float = time.time()

        self._redis.set(key, value)

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)

        self._write_time += time_elapsed
        self._num_objects_written += 1
        self._bytes_written += value_size

        self.log.debug(f'Wrote value of size {value_size} bytes to Redis at key "{key}" in {time_elapsed_ms:,} ms.')

    async def read_value_async(self, key: str)->Any:
        """
        Asynchronously read a value from Redis.
        :param key: the Redis key from which to read the value.

        :return: the value read from Redis.
        """
        start_time: float = time.time()

        value: str|bytes|memoryview = await self._async_redis.get(key)

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)
        value_size = sys.getsizeof(value)

        self._read_time += time_elapsed
        self._num_objects_read += 1
        self._bytes_read += value_size

        self.log.debug(f'Read value of size {value_size} bytes from Redis from key "{key}" in {time_elapsed_ms:,} ms.')

    def read_value(self, key: str)->Any:
        """
        Read a value from Redis from the specified key.
        :param key: the Redis key from which to read the value.

        :return: the value read from Redis.
        """
        start_time: float = time.time()

        value: str|bytes|memoryview = self._redis.get(key)

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)
        value_size = sys.getsizeof(value)

        self._read_time += time_elapsed
        self._num_objects_read += 1
        self._bytes_read += value_size

        self.log.debug(f'Read value of size {value_size} bytes from Redis from key "{key}" in {time_elapsed_ms:,} ms.')

    async def delete_value_async(self, key: str):
        """
        Asynchronously delete the value stored at the specified key from Redis.

        :param key: the name/key of the data to delete
        """
        start_time: float = time.time()

        await self._async_redis.delete(key)

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)

        self._delete_time += time_elapsed
        self._num_objects_deleted += 1

        self.log.debug(f'Deleted value stored at key "{key}" from Redis in {time_elapsed_ms:,} ms.')

    def delete_value(self, key: str):
        """
        Delete the value stored at the specified key from Redis.

        :param key: the name/key of the data to delete
        """
        start_time: float = time.time()

        self._redis.delete(key)

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)

        self._delete_time += time_elapsed
        self._num_objects_deleted += 1

        self.log.debug(f'Deleted value stored at key "{key}" from Redis in {time_elapsed_ms:,} ms.')