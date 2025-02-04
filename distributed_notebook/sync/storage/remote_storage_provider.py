from abc import ABC, abstractmethod

from typing import Any

import logging

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

        self._bytes_read: int = 0
        self._bytes_written: int = 0

        self._num_objects_written: int = 0
        self._num_objects_read: int = 0
        self._num_objects_deleted: int = 0

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
        Return the total time spent reading data from remote storage in seconds.
        """
        return self._read_time

    @property
    def write_time(self) -> float:
        """
        Return the total time spent writing data to remote storage in seconds.
        """
        return self._write_time

    @property
    def delete_time(self) -> float:
        """
        Return the total time spent deleting data from remote storage in seconds.
        """
        return self._delete_time

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
    def is_too_large(self, size_bytes: int)->bool:
        """
        :param size_bytes: the size of the data to (potentially) be written to remote storage
        :return: True if the data is too large to be written, otherwise False
        """
        pass

    @abstractmethod
    async def write_value_async(self, key: str, value: Any):
        """
        Asynchronously write a value to remote storage.

        :param key: the key at which to store the value in remote storage.
        :param value: the value to be written.
        """
        pass

    @abstractmethod
    async def read_value_async(self, key: str)->Any:
        """
        Asynchronously read a value from remote storage.

        :param key: the remote storage key from which to read the value.

        :return: the value read from remote storage.
        """
        pass

    @abstractmethod
    def write_value(self, key: str, value: Any):
        """
        Write a value to remote storage.

        :param key: the key at which to store the value in remote storage.
        :param value: the value to be written.
        """
        pass

    @abstractmethod
    def read_value(self, key: str)->Any:
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