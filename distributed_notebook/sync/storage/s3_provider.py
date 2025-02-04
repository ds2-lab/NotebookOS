from distributed_notebook.sync.storage.storage_provider import RemoteStorageProvider

from typing import Any

import aioboto3
import boto3
DEFAULT_S3_BUCKET_NAME: str = "distributed-notebook-storage"

class S3Provider(RemoteStorageProvider):
    def __init__(self, bucket_name: str = DEFAULT_S3_BUCKET_NAME):
        super().__init__()

        self._s3_client = boto3.client('s3')
        self._bucket_name: str = bucket_name

        self._aio_session: aioboto3.session.Session = aioboto3.Session()

    @property
    def storage_name(self) -> str:
        return "AWS S3"

    async def write_value_async(self, key: str, value: Any):
        """
        Asynchronously write a value to AWS S3.

        :param key: the key at which to store the value in AWS S3.
        :param value: the value to be written.
        """
        pass

    async def read_value_async(self, key: str)->Any:
        """
        Asynchronously read a value from AWS S3.
        :param key: the AWS S3 key from which to read the value.

        :return: the value read from AWS S3.
        """
        pass

    def write_value(self, key: str, value: Any):
        """
        Write a value to AWS S3.

        :param key: the key at which to store the value in AWS S3.
        :param value: the value to be written.
        """
        pass

    def read_value(self, key: str)->Any:
        """
        Read a value from AWS S3.

        :param key: the AWS S3 file key/name from which to read the value.

        :return: the value read from AWS S3.
        """
        pass