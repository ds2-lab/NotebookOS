import io
import sys
import time

from distributed_notebook.sync.storage.remote_storage_provider import RemoteStorageProvider

from typing import Any

import aioboto3
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError


DEFAULT_S3_BUCKET_NAME: str = "distributed-notebook-storage"
DEFAULT_AWS_S3_REGION: str = "us-east-1"

class S3Provider(RemoteStorageProvider):
    def __init__(
            self,
            bucket_name: str = DEFAULT_S3_BUCKET_NAME,
            aws_region: str = DEFAULT_AWS_S3_REGION,
    ):
        super().__init__()

        self._s3_client = boto3.client('s3')
        self._bucket_name: str = bucket_name
        self._aws_region: str = aws_region

        self.init_bucket(bucket_name = bucket_name, aws_region= aws_region)

        self._aio_session: aioboto3.session.Session = aioboto3.Session()

    def init_bucket(self, bucket_name:str = DEFAULT_S3_BUCKET_NAME, aws_region:str = DEFAULT_AWS_S3_REGION):
        """
        This method creates the specified AWS S3 bucket if it does not already exist.
        """
        try:
            # Check if the bucket already exists
            response = self._s3_client.list_buckets()
            buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]

            if bucket_name in buckets:
                print(f"Bucket '{bucket_name}' already exists.")
                return

            # Create the bucket
            create_bucket_params = {'Bucket': bucket_name}

            # Add region if it is specified and is not 'us-east-1' (special rule for AWS S3).
            if aws_region and aws_region != 'us-east-1':
                create_bucket_params['CreateBucketConfiguration'] = {'LocationConstraint': aws_region}

            self._s3_client.create_bucket(**create_bucket_params)
            print(f"Bucket '{bucket_name}' created successfully.")

        except NoCredentialsError:
            print("AWS credentials not found. Please configure them.")
        except PartialCredentialsError:
            print("Incomplete AWS credentials. Please check your configuration.")
        except ClientError as e:
            print(f"Client error: {e}")

    @property
    def storage_name(self) -> str:
        return "AWS S3"

    async def write_value_async(self, key: str, value: Any)->bool:
        """
        Asynchronously write a value to AWS S3 at the specified key.

        :param key: the key at which to store the value in AWS S3.
        :param value: the value to be written.

        :return: True if the write operation is successful, otherwise False.
        """
        if isinstance(value, str):
            value = value.encode('utf-8')
            value_size: int = len(value)
        elif isinstance(value, io.BytesIO):
            value_size: int = value.getbuffer().nbytes
            value = value.getvalue()
        else:
            value_size: int = sys.getsizeof(value)

        async with self._aio_session.client('s3') as s3:
            try:
                start_time: float = time.time()
                await s3.upload_fileobj(Fileobj=io.BytesIO(value), Bucket=self._bucket_name, Key=key)
                time_elapsed: float = time.time() - start_time
            except Exception as e:
                self.log.error(f'Error uploading data of size {value_size} bytes '
                               f'to AWS S3 bucket/key "{self._bucket_name}/{key}": {e}')
                return False

        self.log.debug(f'{value_size} bytes uploaded to AWS S3 bucket/key "{self._bucket_name}/{key}" '
                       f'in {round(time_elapsed, 3):,}ms.')

        self._num_objects_written += 1
        self._write_time += time_elapsed
        self._bytes_written += value_size
        return True

    def write_value(self, key: str, value: Any)->bool:
        """
        Write a value to AWS S3 at the specified key.

        :param key: the key at which to store the value in AWS S3.
        :param value: the value to be written.

        :return: True if the write operation is successful, otherwise False.
        """
        if isinstance(value, str):
            value = value.encode('utf-8')
            value_size: int = len(value)
        elif isinstance(value, io.BytesIO):
            value_size: int = value.getbuffer().nbytes
            value = value.getvalue()
        else:
            value_size: int = sys.getsizeof(value)

        try:
            start_time: float = time.time()
            self._s3_client.upload_fileobj(Fileobj=io.BytesIO(value), Bucket=self._bucket_name, Key=key)
            time_elapsed: float = time.time() - start_time
        except Exception as e:
            self.log.error(f'Error uploading data of size {value_size} bytes '
                           f'to AWS S3 bucket/key "{self._bucket_name}/{key}": {e}')
            return False

        self.log.debug(f'{value_size} bytes uploaded to AWS S3 bucket/key "{self._bucket_name}/{key}" '
                       f'in {round(time_elapsed, 3):,}ms.')

        self._num_objects_written += 1
        self._write_time += time_elapsed
        self._bytes_written += value_size
        return True

    async def read_value_async(self, key: str)->Any:
        """
        Asynchronously read a value from AWS S3.
        :param key: the AWS S3 key from which to read the value.

        :return: the value read from AWS S3.
        """
        start_time: float = time.time()

        async with self._aio_session.client('s3') as s3:
            buffer: io.BytesIO = io.BytesIO()
            try:
                await s3.download_fileobj(self._bucket_name, key, buffer)
                buffer.seek(0) # Need to move pointer back to beginning of buffer.
            except Exception as e:
                self.log.error(f"Error downloading file: {e}")
                raise e  # re-raise

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)
        value_size = buffer.getbuffer().nbytes

        self._read_time += time_elapsed
        self._num_objects_read += 1
        self._bytes_read += value_size

        self.log.debug(f'Read {buffer.getbuffer().nbytes} bytes from AWS S3 bucket/key '
                       f'"{self._bucket_name}/{key}" in {round(time_elapsed_ms, 3):,} ms.')

        return buffer

    def read_value(self, key: str)->Any:
        """
        Read a value from AWS S3 from the specified key.
        :param key: the AWS S3 key from which to read the value.

        :return: the value read from AWS S3.
        """
        start_time: float = time.time()

        buffer: io.BytesIO = io.BytesIO()
        try:
            self._s3_client.download_fileobj(self._bucket_name, key, buffer)
            buffer.seek(0) # Need to move pointer back to beginning of buffer.
        except Exception as e:
            self.log.error(f"Error downloading file: {e}")
            raise e  # re-raise

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)
        value_size = buffer.getbuffer().nbytes

        self._read_time += time_elapsed
        self._num_objects_read += 1
        self._bytes_read += value_size

        self.log.debug(f'Read {buffer.getbuffer().nbytes} bytes from AWS S3 bucket/key '
                       f'"{self._bucket_name}/{key}" in {round(time_elapsed_ms, 3):,} ms.')

        return buffer

    async def delete_value_async(self, key: str)->bool:
        """
        Asynchronously delete the value stored at the specified key from AWS S3.

        :param key: the name/key of the data to delete
        """
        start_time: float = time.time()

        async with self._aio_session.client('s3') as s3:
            try:
                await s3.delete_object(Bucket=self._bucket_name, Key=key)
            except Exception as e:
                self.log.error(f"Error deleting object \"{key}\": {e}")
                return False

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)

        self._delete_time += time_elapsed
        self._num_objects_deleted += 1

        self.log.debug(f'Deleted value stored at key "{key}" from AWS S3 in {time_elapsed_ms:,} ms.')

    def delete_value(self, key: str)->bool:
        """
        Delete the value stored at the specified key from AWS S3.

        :param key: the name/key of the data to delete
        """
        start_time: float = time.time()

        try:
            self._s3_client.delete_object(Bucket=self._bucket_name, Key=key)
        except Exception as e:
            self.log.error(f'Error deleting object "{self._bucket_name}/{key}": {e}')
            return False

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)

        self._delete_time += time_elapsed
        self._num_objects_deleted += 1

        self.log.debug(f'Deleted value stored at key "{key}" from AWS S3 in {time_elapsed_ms:,} ms.')
        
        return True