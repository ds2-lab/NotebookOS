import io
import sys
import time

from distributed_notebook.sync.remote_storage.error import InvalidKeyError
from distributed_notebook.sync.remote_storage.remote_storage_provider import RemoteStorageProvider

from typing import Any

import aioboto3
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError


DEFAULT_S3_BUCKET_NAME: str = "distributed-notebook-remote_storage"
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
        self.log.debug(f'Initializing AWS S3 bucket "{bucket_name}" in AWS region "{aws_region}".')

        try:
            # Check if the bucket already exists
            response = self._s3_client.list_buckets()
            buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]

            if bucket_name in buckets:
                self.log.debug(f'AWS S3 bucket "{bucket_name}" in AWS region "{aws_region}" already exists.')
                return

            # Create the bucket
            create_bucket_params = {'Bucket': bucket_name}

            # Add region if it is specified and is not 'us-east-1' (special rule for AWS S3).
            if aws_region and aws_region != 'us-east-1':
                create_bucket_params['CreateBucketConfiguration'] = {'LocationConstraint': aws_region}

            self.log.debug(f'AWS S3 bucket "{bucket_name}" in AWS region "{aws_region}" does not yet exist.'
                           f'Creating it now.')

            st: float = time.time()
            self._s3_client.create_bucket(**create_bucket_params)
            et: float = time.time()
            time_elapsed: float = et - st

            self.log.debug(f'Successfully created new AWS S3 bucket "{bucket_name}" '
                           f'in AWS region "{aws_region}" in {round(time_elapsed * 1.0e3, 3):,} ms.')

        except NoCredentialsError as ex:
            self.log.error("AWS credentials not found. Please configure them.\n\n")
            raise ex # Re-raise.
        except PartialCredentialsError as ex:
            self.log.error("Incomplete AWS credentials. Please check your configuration.\n\n")
            raise ex # Re-raise.
        except ClientError as ex:
            self.log.error(f'Client error. Failed to initialize AWS S3 bucket "{bucket_name}" '
                           f'in AWS region "{aws_region}" because: {ex}')
            raise ex # Re-raise.

    def is_too_large(self, size_bytes: int)->bool:
        """
        :param size_bytes: the size of the data to (potentially) be written to remote remote_storage
        :return: True if the data is too large to be written, otherwise False
        """
        return False # never too large!

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
            value.seek(0)
            value = value.getbuffer()
            value_size: int = value.nbytes
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

        # Update internal metrics.
        self.update_write_stats(
            time_elapsed_ms=time_elapsed,
            size_bytes=value_size,
            num_values=1
        )

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
            value.seek(0)
            value = value.getbuffer()
            value_size: int = value.nbytes
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

        # Update internal metrics.
        self.update_write_stats(
            time_elapsed_ms=time_elapsed,
            size_bytes=value_size,
            num_values=1
        )

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
            except ClientError as ce:
                response_error = ce.response['Error']
                if response_error['Code'] == 'InvalidAccessKeyId' or response_error['Code'] == '404':
                    raise InvalidKeyError(f'No object with key "{key}" in S3 bucket "{self._bucket_name}"', key = key)
                raise ce # re-raise
            except Exception as e:
                self.log.error(f"Error downloading file: {e}")
                raise e  # re-raise

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)
        value_size = buffer.getbuffer().nbytes

        # Update internal metrics.
        self.update_read_stats(
            time_elapsed_ms=time_elapsed,
            size_bytes=value_size,
            num_values=1
        )

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
        except ClientError as ce:
            response_error = ce.response['Error']
            if response_error['Code'] == 'InvalidAccessKeyId' or response_error['Code'] == '404':
                raise InvalidKeyError(f'No object with key "{key}" in S3 bucket "{self._bucket_name}"', key = key)
            raise ce # re-raise
        except Exception as e:
            self.log.error(f"Error downloading file: {e}")
            raise e  # re-raise

        end_time: float = time.time()
        time_elapsed: float = end_time - start_time
        time_elapsed_ms: float = round(time_elapsed * 1.0e3)
        value_size = buffer.getbuffer().nbytes

        # Update internal metrics.
        self.update_read_stats(
            time_elapsed_ms=time_elapsed,
            size_bytes=value_size,
            num_values=1
        )

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

        self._lifetime_delete_time += time_elapsed
        self._lifetime_num_objects_deleted += 1

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

        self._lifetime_delete_time += time_elapsed
        self._lifetime_num_objects_deleted += 1

        self.log.debug(f'Deleted value stored at key "{key}" from AWS S3 in {time_elapsed_ms:,} ms.')
        
        return True

    async def close_async(self):
        self.close()

    def close(self):
        """Ensure all async coroutines end and clean up."""
        self.log.debug(f"Closing {self.storage_name}.")

        self._s3_client.close()

    get = read_value
    set = write_value