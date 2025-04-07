import logging
from abc import ABC, abstractmethod
from typing import Any, List, Dict

from distributed_notebook.logs import ColoredLogFormatter


class RemoteStorageProvider(ABC):
    """
    RemoteStorageProvider is an abstract base class for any remote storage provider that provides a key-value-like
    API for reading and writing values from and to remote storage (e.g., Redis, AWS S3, memcached, HDFS, etc.).

    For systems providing file-like IO APIs, the file names can be treated as the keys, while the file contents can
    be treated as the values.
    """

    def __init__(self):
        self.log = logging.getLogger(__class__.__name__)
        self.log.handlers.clear()
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

        self._write_time: float = 0
        self._read_time: float = 0
        self._delete_time: float = 0

        self._write_times: List[float] = []
        self._read_times: List[float] = []
        self._delete_times: List[float] = []

        self._bytes_read: int = 0
        self._bytes_written: int = 0

        self._num_objects_written: int = 0
        self._num_objects_read: int = 0
        self._num_objects_deleted: int = 0

        self._lifetime_write_time: float = 0
        self._lifetime_read_time: float = 0
        self._lifetime_delete_time: float = 0

        self._lifetime_bytes_read: int = 0
        self._lifetime_bytes_written: int = 0

        self._lifetime_num_objects_written: int = 0
        self._lifetime_num_objects_read: int = 0
        self._lifetime_num_objects_deleted: int = 0

        self._lifetime_write_times: List[float] = []
        self._lifetime_read_times: List[float] = []
        self._lifetime_delete_times: List[float] = []

    @property
    def num_objects_written(self) -> int:
        """
        :return: the number of objects written to remote storage.
        """
        return self._num_objects_written

    @property
    def num_objects_read(self) -> int:
        """
        :return: the number of objects read from remote storage.
        """
        return self._num_objects_read

    @property
    def num_objects_deleted(self) -> int:
        """
        :return: the number of objects deleted from remote storage.
        """
        return self._num_objects_deleted

    @property
    def bytes_read(self) -> int:
        """
        :return: the number of bytes read from remote storage.
        """
        return self._bytes_read

    @property
    def bytes_written(self) -> int:
        """
        :return: the number of bytes written to remote storage.
        """
        return self._bytes_written

    @property
    def read_time(self) -> float:
        """
        Return the total time spent reading data from remote storage in milliseconds.
        """
        return self._read_time

    @property
    def write_time(self) -> float:
        """
        Return the total time spent writing data to remote storage in milliseconds.
        """
        return self._write_time

    @property
    def delete_time(self) -> float:
        """
        Return the total time spent deleting data from remote storage in milliseconds.
        """
        return self._delete_time

    @property
    def read_times(self) -> List[float]:
        """
        Return the individual times spent reading data from remote storage in milliseconds.
        """
        return self._read_times

    @property
    def write_times(self) -> List[float]:
        """
        Return the individual times spent writing data to remote storage in milliseconds.
        """
        return self._write_times

    @property
    def delete_times(self) -> List[float]:
        """
        Return the individual times spent deleting data from remote storage in milliseconds.
        """
        return self._delete_times

    @property
    def lifetime_num_objects_written(self) -> int:
        """
        :return: the number of objects written to remote storage.
        """
        return self._lifetime_num_objects_written

    @property
    def lifetime_num_objects_read(self) -> int:
        """
        :return: the number of objects read from remote storage.
        """
        return self._lifetime_num_objects_read

    @property
    def lifetime_num_objects_deleted(self) -> int:
        """
        :return: the number of objects deleted from remote storage.
        """
        return self._lifetime_num_objects_deleted

    @property
    def lifetime_bytes_read(self) -> int:
        """
        :return: the number of bytes read from remote storage.
        """
        return self._lifetime_bytes_read

    @property
    def lifetime_bytes_written(self) -> int:
        """
        :return: the number of bytes written to remote storage.
        """
        return self._lifetime_bytes_written

    @property
    def lifetime_read_time(self) -> float:
        """
        Return the total time spent reading data from remote storage in seconds.
        """
        return self._lifetime_read_time

    @property
    def lifetime_write_time(self) -> float:
        """
        Return the total time spent writing data to remote storage in seconds.
        """
        return self._lifetime_write_time

    @property
    def lifetime_delete_time(self) -> float:
        """
        Return the total time spent deleting data from remote storage in seconds.
        """
        return self._lifetime_delete_time

    @property
    def lifetime_read_times(self) -> List[float]:
        """
        Return the individual times spent reading data from remote storage in seconds.
        """
        return self._lifetime_read_times

    @property
    def lifetime_write_times(self) -> List[float]:
        """
        Return the individual times spent writing data to remote storage in seconds.
        """
        return self._lifetime_write_times

    @property
    def lifetime_delete_times(self) -> List[float]:
        """
        Return the individual times spent deleting data from remote storage in seconds.
        """
        return self._lifetime_delete_times

    def clear_statistics(self):
        """
        This method resets all statistics that aren't prefixed with "lifetime" to 0.
        """
        self._write_time = 0
        self._read_time = 0
        self._delete_time = 0

        self._bytes_read = 0
        self._bytes_written = 0

        self._num_objects_written = 0
        self._num_objects_read = 0
        self._num_objects_deleted = 0

        self._write_times.clear()
        self._read_times.clear()
        self._delete_times.clear()

    def update_write_stats(self, time_elapsed_ms: float, size_bytes: int, num_values: int = 1):
        """
        Updates the write-related metrics of the RedisProvider.

        :param time_elapsed_ms: the time taken by the write operation.
        :param size_bytes: the size, in bytes, of the data written to Redis.
        :param num_values: the number of objects written. This will usually be 1, but if a large object is
        chunked, then this should be the number of individual chunks.
        """
        self._write_time += time_elapsed_ms
        self._num_objects_written += num_values
        self._bytes_written += size_bytes

        self._lifetime_num_objects_written += num_values
        self._lifetime_write_time += time_elapsed_ms
        self._lifetime_bytes_written += size_bytes

        self._write_times.append(time_elapsed_ms)
        self._lifetime_write_times.append(time_elapsed_ms)

    def update_read_stats(self, time_elapsed_ms: float, size_bytes: int, num_values: int = 1):
        """
        Updates the write-related metrics of the RedisProvider.

        :param time_elapsed_ms: the time taken by the read operation.
        :param size_bytes: the size, in bytes, of the data read from Redis.
        :param num_values: the number of objects read. This will usually be 1, but if a large object is
        chunked, then this should be the number of individual chunks.
        """
        self._read_time += time_elapsed_ms
        self._num_objects_read += num_values
        self._bytes_read += size_bytes

        self._lifetime_read_time += time_elapsed_ms
        self._lifetime_num_objects_read += num_values
        self._lifetime_bytes_read += size_bytes

        self._read_times.append(time_elapsed_ms)
        self._lifetime_read_times.append(time_elapsed_ms)

    def update_delete_stats(self, time_elapsed_ms: float, num_values: int = 1):
        """
        Updates the write-related metrics of the RedisProvider.

        :param time_elapsed_ms: the time taken by the read operation.
        :param num_values: the number of objects read. This will usually be 1, but if a large object is
        chunked, then this should be the number of individual chunks.
        """
        self._delete_time += time_elapsed_ms
        self._num_objects_deleted += num_values

        self._lifetime_delete_time += time_elapsed_ms
        self._lifetime_num_objects_deleted += num_values

        self._delete_times.append(time_elapsed_ms)
        self._lifetime_delete_times.append(time_elapsed_ms)

    @property
    @abstractmethod
    def storage_name(self) -> str:
        """
        :return: a human-readable name of the remote storage whose access is
        provided by objects of this class.
        """
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    async def close_async(self):
        pass

    @abstractmethod
    def is_too_large(self, size_bytes: int) -> bool:
        """
        :param size_bytes: the size of the data to (potentially) be written to remote storage
        :return: True if the data is too large to be written, otherwise False
        """
        pass

    @abstractmethod
    async def write_value_async(self, key: str, value: Any, size_bytes: int = -1)->bool:
        """
        Asynchronously write a value to remote storage.

        :param key: the key at which to store the value in remote storage.
        :param value: the value to be written.
        :param size_bytes: the known size of the data to be written, if available.
        """
        pass

    @abstractmethod
    async def read_value_async(self, key: str) -> Any:
        """
        Asynchronously read a value from remote storage.

        :param key: the remote storage key from which to read the value.

        :return: the value read from remote storage.
        """
        pass

    @abstractmethod
    def write_value(self, key: str, value: Any, size_bytes: int = -1)->bool:
        """
        Write a value to remote storage.

        :param key: the key at which to store the value in remote storage.
        :param value: the value to be written.
        :param size_bytes: the known size of the data to be written, if available.
        """
        pass

    @abstractmethod
    def read_value(self, key: str) -> Any:
        """
        Read a value from remote storage.

        :param key: the remote storage key from which to read the value.

        :return: the value read from remote storage.
        """
        pass

    @abstractmethod
    async def delete_value_async(self, key: str):
        """
        Asynchronously delete the value stored at the specified key from remote storage.

        :param key: the name/key of the data to delete
        """
        pass

    @abstractmethod
    def delete_value(self, key: str):
        """
        Delete the value stored at the specified key from remote storage.

        :param key: the name/key of the data to delete
        """
        pass
