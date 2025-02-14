import asyncio
import io
from typing import Any, Optional, List, ByteString

import redis
import redis.asyncio as async_redis
import sys
import time

from distributed_notebook.sync.checkpointing.util import split_bytes_buffer
from distributed_notebook.sync.remote_storage.error import InvalidKeyError
from distributed_notebook.sync.remote_storage.remote_storage_provider import RemoteStorageProvider

try:
    import fakeredis

    fakeredis_imported: bool = True
except ImportError:
    fakeredis_imported: bool = False


class RedisProvider(RemoteStorageProvider):
    # We automatically chunk values whose size is greater than this.
    size_limit_bytes: int = 400.0e6

    def __init__(
            self,
            host: str = "",
            port: int = 6379,
            db: int = 0,
            password: Optional[str] = None,
            additional_redis_args: Optional[dict] = None,
            redis_client=None,  # For unit testing
            async_redis_client=None,  # For unit testing
            strict_size_checking_during_tests: bool = False,
    ):
        super().__init__()

        if ':' in host:
            host_orig: str = host
            idx = host_orig.index(':')
            host = host_orig[:idx]

            try:
                port = int(host_orig[idx + 1:])  # +1 because idx is the index of the ':', not the beginning of the port
            except IndexError:
                # Apparently there's nothing after the colon, so we'll use the default of 6379 (or whatever was passed).
                pass

        self._redis_password: str = password
        self._redis_db: int = db
        self._redis_port = port
        self._redis_host = host

        # Cached. Just used for logging.
        self._size_limit_mb: float = RedisProvider.size_limit_bytes / 1.0e6

        # If this is true, then our is_too_large will operate as if our clients are not FakeRedis and FakeAsyncRedis
        # (which they are while unit testing). That is, we normally just allow objects of any size while unit testing.
        # But if we want to test something that requires us to be strict about object sizes, then we set this to true.
        self._strict_size_checking_during_tests: bool = strict_size_checking_during_tests

        if additional_redis_args is None:
            additional_redis_args = dict()

        self._additional_redis_args: dict[str, Any] = additional_redis_args

        if redis_client is not None:
            self._redis = redis_client
        else:
            self.log.debug(f"Creating synchronous Redis client of Redis server at {host}:{port} (db={db}).")
            self._redis = redis.Redis(host=host, port=port, db=db, password=password, **additional_redis_args)

        if async_redis_client is not None:
            self._async_redis = async_redis_client
        else:
            self.log.debug(f"Creating asynchronous Redis client of Redis server at {host}:{port} (db={db}).")
            self._async_redis = async_redis.Redis(host=host, port=port, db=db, password=password,
                                                  **additional_redis_args)

        self.log.debug(f"Successfully connected to Redis server at {host}:{port} (db={db}).")

    @property
    def hostname(self) -> str:
        return self._redis_host

    @property
    def redis_port(self) -> int:
        return self._redis_port

    def __ensure_async_redis(self) -> bool:
        """
        Ensure the RedisProvider has created its async Redis client.

        :return: true if the async Redis client already existed, false if the async Redis client did not already exist.
        """
        if getattr(self, "_async_redis") is None:
            self._async_redis = async_redis.Redis(
                host=self._redis_host,
                port=self._redis_port,
                db=self._redis_db,
                password=self._redis_password,
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
                host=self._redis_host,
                port=self._redis_port,
                db=self._redis_db,
                password=self._redis_password,
                **self._additional_redis_args
            )

    @property
    def storage_name(self) -> str:
        return f"Redis({self._redis_host}:{self._redis_port},db={self._redis_db})"

    def is_too_large(self, size_bytes: int) -> bool:
        """
        :param size_bytes: the size of the data to (potentially) be written to remote remote_storage
        :return: True if the data is too large to be written, otherwise False
        """
        if self._strict_size_checking_during_tests:
            # Even if we're not unit testing, we will just perform strict size checking.
            return size_bytes > RedisProvider.size_limit_bytes

        global fakeredis_imported
        if not fakeredis_imported:
            # The FakeRedis module isn't installed, so just perform strict size checking.
            return size_bytes > RedisProvider.size_limit_bytes

        # If we were able to import FakeRedis (which is only a dev dependency and may fail for non-development
        # installations), and our redis clients are instances of the FakeRedis and FakeAsyncRedis classes,
        # then we'll just return True.
        if isinstance(self._redis, fakeredis.FakeRedis) and isinstance(self._async_redis, fakeredis.FakeAsyncRedis):
            self.log.warning(f"We appear to be unit testing, so returning 'False' for 'is_too_large({size_bytes:,}) "
                             f"despite it being >512MB...")
            return False  # Allow objects of arbitrary sizes for unit testing (when the flag mentioned above is False).

        return size_bytes > RedisProvider.size_limit_bytes

    async def __chunk_data_async(self, key: str, value: bytes, size_bytes: int = -1, size_mb: float = -1) -> bool:
        """
        Split the given buffer into smaller pieces so that it can be written to remote storage.

        This is used when our remote storage provider has a size limit for values.

        :param key: the base key.
        :param value: the value to be written.
        :param size_bytes: the size of the value to be written in bytes.
        :param size_mb: the size of the value to be written in megabytes.
        :return:
        """
        if size_bytes <= 0:
            size_bytes = len(value)

        if size_mb <= 0:
            size_mb = size_bytes / 1.0e6

        chunks: List[ByteString] = split_bytes_buffer(value)  # Default chunk_size is 128MB.
        chunk_sizes: List[str] = [f'{round(len(chunk) / 1.0e6, 3):,} MB' for chunk in chunks]

        self.log.debug(f'Split value of size {size_mb:,} MB to be stored at key '
                       f'"{key}" into {len(chunks)} chunks of size 128MB each. '
                       f'Actual chunk sizes: {",".join(chunk_sizes)}')

        start_time: float = time.time()
        await self._async_redis.lpush(key, *chunks)
        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)

        # Update internal metrics.
        self.update_write_stats(
            time_elapsed_ms=time_elapsed,
            size_bytes=size_bytes,
            num_values=len(chunks),
        )

        self.log.debug(f'Wrote {len(chunks)} chunks with total size of {size_mb:,} MB '
                       f'to Redis at key "{key}" in {time_elapsed_ms:,} ms.')

        return True

    def __chunk_data(self, key: str, value: bytes, size_bytes: int = -1, size_mb: float = -1) -> bool:
        """
        Split the given buffer into smaller pieces so that it can be written to remote storage.

        This is used when our remote storage provider has a size limit for values.

        :param key: the base key.
        :param value: the value to be written.
        :param size_bytes: the size of the value to be written in bytes.
        :param size_mb: the size of the value to be written in megabytes.
        :return:
        """
        if size_bytes <= 0:
            size_bytes = len(value)

        if size_mb <= 0:
            size_mb = size_bytes / 1.0e6

        chunks: List[ByteString] = split_bytes_buffer(value)  # Default chunk_size is 128MB.
        chunk_sizes: List[str] = [f'{len(chunk) / 1.0e6:,} MB' for chunk in chunks]

        self.log.debug(f'Split value of size {size_mb:,} MB to be stored at key '
                       f'"{key}" into {len(chunks)} chunks of size 128MB each. '
                       f'Actual chunk sizes: {",".join(chunk_sizes)}')

        start_time: float = time.time()
        self._redis.lpush(key, *chunks)
        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)

        # Update internal metrics.
        self.update_write_stats(
            time_elapsed_ms=time_elapsed,
            size_bytes=size_bytes,
            num_values=len(chunks),
        )

        self.log.debug(f'Wrote {len(chunks)} chunks with total size of {size_mb:,} MB '
                       f'to Redis at key "{key}" in {time_elapsed_ms:,} ms.')

        return True

    async def write_value_async(self, key: str, value: Any, size_bytes:int = -1) -> bool:
        """
        Asynchronously write a value to Redis at the specified key.

        :param size_bytes: the known size of the data to be written, if available.
        :param key: the key at which to store the value in Redis.
        :param value: the value to be written.
        """
        self.__ensure_async_redis()

        if size_bytes <= 0:
            if isinstance(value, bytes):
                size_bytes = len(value)
            elif isinstance(value, io.BytesIO):
                value.seek(0)
                size_bytes = value.getbuffer().nbytes
            else:
                size_bytes = sys.getsizeof(value)

        size_mb: float = size_bytes / 1.0e6

        if self.is_too_large(size_bytes):
            self.log.warning(f'Cannot write value (of type "{type(value).__name__}") with key="{key}" '
                             f'to {self.storage_name}. Model state is larger than maximum size of '
                             f'{self._size_limit_mb:,} MB: {size_mb:,} MB.')

            return await self.__chunk_data_async(key, value, size_mb=size_mb)

        self.log.debug(f'Writing value of type "{type(value).__name__}" size {size_bytes:,} '
                       f'bytes to Redis at key "{key}".')

        start_time: float = time.time()

        await self._async_redis.set(key, value)

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)

        # Update internal metrics.
        self.update_write_stats(
            time_elapsed_ms=time_elapsed,
            size_bytes=size_bytes
        )

        self.log.debug(f'Wrote value of size {size_bytes} bytes to Redis at key '
                       f'"{key}" in {time_elapsed_ms:,} ms.')

        return True

    def write_value(self, key: str, value: Any, size_bytes:int = -1) -> bool:
        """
        Write a value to Redis at the specified key.

        :param size_bytes: the known size of the data to be written, if available.
        :param key: the key at which to store the value in Redis.
        :param value: the value to be written.
        """
        self.__ensure_redis()

        if size_bytes <= 0:
            if isinstance(value, bytes):
                size_bytes = len(value)
            elif isinstance(value, io.BytesIO):
                value.seek(0)
                size_bytes = value.getbuffer().nbytes
            else:
                size_bytes = sys.getsizeof(value)

        size_mb: float = size_bytes / 1.0e6

        if self.is_too_large(size_bytes):
            self.log.warning(f'Cannot write value (of type "{type(value).__name__}") with key="{key}" '
                             f'to {self.storage_name}. Model state is larger than maximum size of '
                             f'{self._size_limit_mb:,} MB: {size_mb:,} MB.')

            return self.__chunk_data(key, value, size_mb=size_mb)

        self.log.debug(f'Writing value of type "{type(value).__name__}" size {size_bytes:,} '
                       f'bytes to Redis at key "{key}".')

        start_time: float = time.time()

        self._redis.set(key, value)

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)

        # Update internal metrics.
        self.update_write_stats(
            time_elapsed_ms=time_elapsed,
            size_bytes=size_bytes
        )

        self.log.debug(f'Wrote value of size {size_bytes} bytes to Redis at key '
                       f'"{key}" in {time_elapsed_ms:,} ms.')

        return True

    async def read_value_async(self, key: str) -> Any:
        """
        Asynchronously read a value from Redis.
        :param key: the Redis key from which to read the value.

        :return: the value read from Redis.
        """
        self.__ensure_async_redis()

        start_time: float = time.time()

        # Get the type of the data.
        value_type: str | bytes = await self._async_redis.type(key)

        if isinstance(value_type, bytes):
            value_type = value_type.decode()

        # If the value type is "none", then there's simply no data stored at
        # that key, in which case we can already raise an InvalidKeyError.
        if value_type== "none":
            self.log.debug(f'Type of value at key "{key}" is "none".')
            raise InvalidKeyError(f'No data stored in Redis at key "{key}"')

        if value_type != "string" and value_type != "list":
            self.log.error(f'Value stored in Redis at key "{key}" has '
                           f'unexpected type: "{value_type}". '
                           f'Expected "string" or "list".')
            raise ValueError(f'Value stored in Redis at key "{key}" has '
                             f'unexpected type: "{value_type}". '
                             f'Expected "string" or "list".')

        num_values_read: int = 1
        if value_type == "list":
            # Read the entire list.
            values: Optional[List[str | bytes | memoryview]] = await self._async_redis.lrange(key, 0, -1)
            if values is None or len(values) == 0:
                raise InvalidKeyError(f'No data stored in Redis at key "{key}"')

            self.log.debug(f'Read {len(values)} value(s) from list stored in '
                           f'Redis at key "{key}" in {round((time.time() - start_time) * 1.0e3):,} ms.')

            num_values_read = len(values)

            # Concatenate all the items in the list together.
            value: str | bytes | memoryview = b''.join(values)
        else:
            value: Optional[str | bytes | memoryview] = await self._async_redis.get(key)

            if value is None:
                raise InvalidKeyError(f'No data stored in Redis at key "{key}"')

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)
        value_size = sys.getsizeof(value)

        # Update internal metrics.
        self.update_read_stats(
            time_elapsed_ms=time_elapsed,
            size_bytes=value_size,
            num_values=num_values_read
        )

        self.log.debug(f'Read value of size {value_size} bytes from Redis from key "{key}" in {time_elapsed_ms:,} ms.')

        return value

    def read_value(self, key: str) -> Any:
        """
        Read a value from Redis from the specified key.
        :param key: the Redis key from which to read the value.

        :return: the value read from Redis.
        """
        self.__ensure_redis()

        start_time: float = time.time()

        # Get the type of the data.
        value_type: str | bytes = self._redis.type(key)

        if isinstance(value_type, bytes):
            value_type = value_type.decode()

        # If the value type is "none", then there's simply no data stored at
        # that key, in which case we can already raise an InvalidKeyError.
        if value_type== "none":
            self.log.debug(f'Type of value at key "{key}" is "none".')
            raise InvalidKeyError(f'No data stored in Redis at key "{key}"')

        if value_type != "string" and value_type != "list":
            self.log.error(f'Value stored in Redis at key "{key}" has '
                           f'unexpected type: "{value_type}". '
                           f'Expected "string" or "list".')
            raise ValueError(f'Value stored in Redis at key "{key}" has '
                             f'unexpected type: "{value_type}". '
                             f'Expected "string" or "list".')

        num_values_read: int = 1

        if value_type == "list":
            # Read the entire list.
            values: Optional[List[str | bytes | memoryview]] = self._redis.lrange(key, 0, -1)
            if values is None or len(values) == 0:
                raise InvalidKeyError(f'No data stored in Redis at key "{key}"')

            self.log.debug(f'Read {len(values)} value(s) from list stored in '
                           f'Redis at key "{key}" in {round((time.time() - start_time) * 1.0e3):,} ms.')

            num_values_read = len(values)

            # Concatenate all the items in the list together.
            value: str | bytes | memoryview = b''.join(values)
        else:
            value: Optional[str | bytes | memoryview] = self._redis.get(key)

            if value is None:
                raise InvalidKeyError(f'No data stored in Redis at key "{key}"')

        if value is None:
            raise InvalidKeyError(f'No data stored in Redis at key "{key}"')

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)
        value_size = sys.getsizeof(value)

        # Update internal metrics.
        self.update_read_stats(
            time_elapsed_ms=time_elapsed,
            size_bytes=value_size,
            num_values=num_values_read
        )

        self.log.debug(f'Read value of size {value_size} bytes from Redis from key "{key}" in {time_elapsed_ms:,} ms.')

        return value

    async def delete_value_async(self, key: str) -> bool:
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

        self.update_delete_stats(time_elapsed, 1)

        self.log.debug(f'Deleted value stored at key "{key}" from Redis in {time_elapsed_ms:,} ms.')

        return True

    def delete_value(self, key: str) -> bool:
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

        self.update_delete_stats(time_elapsed, 1)

        self.log.debug(f'Deleted value stored at key "{key}" from Redis in {time_elapsed_ms:,} ms.')

        return True

    async def close_async(self):
        self.log.debug(f"Closing {self.storage_name}.")

        self._redis.close()
        await self._async_redis.close()

    def close(self):
        """Ensure all async coroutines end and clean up."""
        self.log.debug(f"Closing {self.storage_name}.")

        self._redis.close()

        try:
            asyncio.run(self._async_redis.close())
        except RuntimeError as ex:
            self.log.debug(f"RuntimeError occurred while closing RedisLog: {ex}")

    get = read_value
    set = write_value
