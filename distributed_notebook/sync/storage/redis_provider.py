import sys
import time
from typing import Any, Optional

import fakeredis
import redis
import redis.asyncio as async_redis

from distributed_notebook.sync.storage.remote_storage_provider import RemoteStorageProvider


class RedisProvider(RemoteStorageProvider):
    def __init__(
            self,
            host:str = "",
            port: int = 6379,
            db: int = 0,
            password: Optional[str] = None,
            additional_redis_args: Optional[dict] = None,
            redis_client: Optional[redis.Redis | fakeredis.FakeRedis] = None, # For unit testing
            async_redis_client: Optional[async_redis.Redis | fakeredis.FakeAsyncRedis] = None, # For unit testing
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

        if redis_client is not None:
            self._redis = redis_client
        else:
            self.log.debug(f"Creating synchronous Redis client of Redis server at {host}:{port} (db={db}).")
            self._redis = redis.Redis(host = host, port = port, db = db, password = password, **additional_redis_args)

        if async_redis_client is not None:
            self._async_redis = async_redis_client
        else:
            self.log.debug(f"Creating asynchronous Redis client of Redis server at {host}:{port} (db={db}).")
            self._async_redis = async_redis.Redis(host = host, port = port, db = db, password = password, **additional_redis_args)


    def __ensure_async_redis(self)->bool:
        """
        Ensure the RedisProvider has created its async Redis client.

        :return: true if the async Redis client already existed, false if the async Redis client did not already exist.
        """
        if getattr(self, "_async_redis") is None:
            self._async_redis = async_redis.Redis(
                host = self._redis_host,
                port = self._redis_port,
                db = self._redis_db,
                password = self._redis_password,
                **self._additional_redis_args
            )
            return False
        else:
            return True

    def __ensure_redis(self):
        """
        Ensure the RedisProvider has created its synchronous Redis client.

        :return: true if the synchronous Redis client already existed,
                 false if the synchronous Redis client did not already exist.
        """
        if getattr(self, "_redis") is None:
            self._redis = redis.Redis(
                host = self._redis_host,
                port = self._redis_port,
                db = self._redis_db,
                password = self._redis_password,
                **self._additional_redis_args
            )

    @property
    def storage_name(self)->str:
        return f"Redis({self._redis_host}:{self._redis_port},db={self._redis_db})"

    def is_too_large(self, size_bytes: int)->bool:
        """
        :param size_bytes: the size of the data to (potentially) be written to remote storage
        :return: True if the data is too large to be written, otherwise False
        """
        return size_bytes > 512e6

    async def write_value_async(self, key: str, value: Any)->bool:
        """
        Asynchronously write a value to Redis at the specified key.

        :param key: the key at which to store the value in Redis.
        :param value: the value to be written.
        """
        self.__ensure_async_redis()

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

        return True

    def write_value(self, key: str, value: Any)->bool:
        """
        Write a value to Redis at the specified key.

        :param key: the key at which to store the value in Redis.
        :param value: the value to be written.
        """
        self.__ensure_redis()

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

        return True

    async def read_value_async(self, key: str)->Any:
        """
        Asynchronously read a value from Redis.
        :param key: the Redis key from which to read the value.

        :return: the value read from Redis.
        """
        self.__ensure_async_redis()

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

        return value

    def read_value(self, key: str)->Any:
        """
        Read a value from Redis from the specified key.
        :param key: the Redis key from which to read the value.

        :return: the value read from Redis.
        """
        self.__ensure_redis()

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

        return value

    async def delete_value_async(self, key: str)->bool:
        """
        Asynchronously delete the value stored at the specified key from Redis.

        :param key: the name/key of the data to delete
        """
        self.__ensure_async_redis()

        start_time: float = time.time()

        await self._async_redis.delete(key)

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)

        self._delete_time += time_elapsed
        self._num_objects_deleted += 1

        self.log.debug(f'Deleted value stored at key "{key}" from Redis in {time_elapsed_ms:,} ms.')

        return True

    def delete_value(self, key: str)->bool:
        """
        Delete the value stored at the specified key from Redis.

        :param key: the name/key of the data to delete
        """
        self.__ensure_redis()

        start_time: float = time.time()

        self._redis.delete(key)

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)

        self._delete_time += time_elapsed
        self._num_objects_deleted += 1

        self.log.debug(f'Deleted value stored at key "{key}" from Redis in {time_elapsed_ms:,} ms.')

        return True