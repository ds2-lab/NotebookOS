import io
import os
import time
from typing import Any, Dict, Optional

import aioboto3
import boto3
import botocore
import torch

from distributed_notebook.sync.checkpointing.pointer import DatasetPointer, ModelPointer
from distributed_notebook.sync.checkpointing.remote_checkpointer import RemoteCheckpointer

DEFAULT_S3_BUCKET_NAME: str = "distributed-notebook-storage"


class S3Checkpointer(RemoteCheckpointer):
    def __init__(self, bucket_name: str = DEFAULT_S3_BUCKET_NAME):
        super().__init__()

        self._s3_client = boto3.client('s3')
        self._bucket_name: str = bucket_name

        self._aio_session: aioboto3.session.Session = aioboto3.Session()

        self._num_objects_written: int = 0
        self._num_objects_read: int = 0
        self._num_objects_deleted: int = 0

    @property
    def num_objects_written(self) -> int:
        return self._num_objects_written

    @property
    def num_objects_read(self) -> int:
        return self._num_objects_read

    @property
    def num_objects_deleted(self) -> int:
        return self._num_objects_deleted

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    def delete_data(self, object_name: str)->bool:
        """
        Delete a file in the S3 bucket.
        :param object_name: the name/key of the file to delete
        :return: True if the file was deleted successfully, otherwise False.
        """
        try:
            self._s3_client.delete_object(Bucket=self._bucket_name, Key=object_name)
            self._num_objects_deleted += 1
            return True
        except Exception as e:
            self.log.error(f"Error deleting object \"{object_name}\": {e}")
            return False

    async def delete_data_async(self, object_name: str)->bool:
        """
        Delete a file in the S3 bucket.
        :param object_name: the name/key of the file to delete
        :return: True if the file was deleted successfully, otherwise False.
        """
        try:
            self._s3_client.delete_object(Bucket=self._bucket_name, Key=object_name)
            self._num_objects_deleted += 1
            return True
        except Exception as e:
            self.log.error(f"Error deleting object \"{object_name}\": {e}")
            return False

    async def upload_bytes_to_s3_async(self, data, object_name: str):
        """
        Upload in-memory bytes to an S3 bucket.

        :param data: Data to upload (bytes or memoryview)
        :param object_name: S3 object name
        :return: True if data was uploaded, else False
        """
        if isinstance(data, str):
            data = data.encode('utf-8')
        elif isinstance(data, io.BytesIO):
            data = data.getvalue()

        async with self._aio_session.client('s3') as s3:
            try:
                s3.upload_fileobj(Fileobj=io.BytesIO(data), Bucket=self._bucket_name, Key=object_name)
                self.log.debug(f"Data uploaded to {self._bucket_name}/{object_name}")
                self._num_objects_written += 1
                return True
            except Exception as e:
                self.log.error(f"Error uploading data: {e}")
                return False

    def upload_bytes_to_s3(self, data, object_name: str)->bool:
        """
        Upload in-memory bytes to an S3 bucket.

        :param data: Data to upload (bytes or memoryview)
        :param object_name: S3 object name
        :return: True if data was uploaded, else False
        """
        if isinstance(data, str):
            data = data.encode('utf-8')
        elif isinstance(data, io.BytesIO):
            data = data.getvalue()

        try:
            self._s3_client.upload_fileobj(Fileobj=io.BytesIO(data), Bucket=self._bucket_name, Key=object_name)
            self.log.debug(f"Data uploaded to {self._bucket_name}/{object_name}")
            self._num_objects_written += 1
            return True
        except Exception as e:
            self.log.error(f"Error uploading data: {e}")
            raise e # Re-raise

    async def download_file_from_s3_async(self, object_name: str) -> io.BytesIO:
        """
        Download a file from an S3 bucket.

        :param object_name: S3 object name
        :return: True if file was downloaded, else False
        """
        async with self._aio_session.client('s3') as s3:
            buffer: io.BytesIO = io.BytesIO()
            try:
                s3.download_fileobj(self._bucket_name, object_name, buffer)
                self._num_objects_read += 1
                return buffer
            except Exception as e:
                self.log.error(f"Error downloading file: {e}")
                raise e  # re-raise

    def download_file_from_s3(self, object_name: str) -> io.BytesIO:
        """
        Download a file from an S3 bucket.

        :param object_name: S3 object name
        :return: True if file was downloaded, else False
        """
        buffer: io.BytesIO = io.BytesIO()
        try:
            self._s3_client.download_fileobj(self._bucket_name, object_name, buffer)
            self._num_objects_read += 1
            return buffer
        except Exception as e:
            self.log.error(f"Error downloading file: {e}")
            raise e  # re-raise

    async def __read_state_dict_async(self, object_name: str, model_name: str) -> Optional[Dict[str, Any]]:
        """
        Read a single state dictionary from AWS S3.

        :param object_name: the AWS S3 object name.
        :param model_name: the name of the model associated with the state dictionary
                           that we've been instructed to read.
        :return:
        """
        try:
            st: float = time.time()
            buffer: io.BytesIO = await self.download_file_from_s3_async(object_name)
            et: float = time.time()
        except Exception as ex:
            self.log.error(f"Failed to read state of model \"{model_name}\" from AWS S3 at bucket/key "
                           f"\"{os.path.join(self._bucket_name, object_name)}\" because: {ex}")
            raise ex  # re-raise

        self.log.debug(f"Successfully read state of model \"{model_name}\" to AWS S3 at bucket/key "
                       f"\"{os.path.join(self._bucket_name, object_name)}\" (model size: "
                       f"{buffer.getbuffer().nbytes / 1.0e6} MB) in {et - st} seconds.")

        try:
            state_dict: Dict[str, Any] = torch.load(buffer)
        except Exception as ex:
            self.log.error(f"Failed to load state of model \"{model_name}\" from data retrieved from AWS S3 at "
                           f"bucket/key \"{os.path.join(self._bucket_name, object_name)}\"because: {ex}.")
            raise ex  # re-raise

        return state_dict

    def __read_state_dict(self, object_name: str, model_name: str) -> Optional[Dict[str, Any]]:
        """
        Read a single state dictionary from AWS S3.

        :param object_name: the AWS S3 object name.
        :param model_name: the name of the model associated with the state dictionary that we've been instructed to read.
        :return:
        """
        try:
            st: float = time.time()
            buffer: io.BytesIO = self.download_file_from_s3(object_name)
            et: float = time.time()
        except Exception as ex:
            self.log.error(
                f"Failed to read state of model \"{model_name}\" from AWS S3 at bucket/key \"{os.path.join(self._bucket_name, object_name)}\" "
                f"because: {ex}")
            raise ex  # re-raise

        self.log.debug(
            f"Successfully read state of model \"{model_name}\" to AWS S3 at bucket/key \"{os.path.join(self._bucket_name, object_name)}\" "
            f"(model size: {buffer.getbuffer().nbytes / 1.0e6} MB) in {et - st} seconds.")

        try:
            state_dict: Dict[str, Any] = torch.load(buffer)
        except Exception as ex:
            self.log.error(f"Failed to load state of model \"{model_name}\" from data retrieved from AWS S3 at "
                           f"bucket/key \"{os.path.join(self._bucket_name, object_name)}\"because: {ex}.")
            raise ex  # re-raise

        return state_dict

    async def read_state_dicts_async(self, pointer: ModelPointer) -> tuple[
        Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:

        if pointer is None:
            raise ValueError("cannot read model using nil ModelPointer")

        model_name: str = pointer.large_object_name
        base_object_name: str = pointer.key

        model_object_name: str = os.path.join(base_object_name, "model.pt")
        optimizer_object_name: str = os.path.join(base_object_name, "optimizer.pt")
        criterion_object_name: str = os.path.join(base_object_name, "criterion.pt")
        constructor_args_key: str = os.path.join(base_object_name, "constructor_args.pt")

        try:
            model_state_dict = self.__read_state_dict(model_object_name, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read model state dictionary from AWS S3: {ex}")
            raise ex  # re-raise

        try:
            optimizer_state_dict = self.__read_state_dict(optimizer_object_name, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read optimizer state dictionary from AWS S3: {ex}")
            raise ex  # re-raise

        try:
            criterion_state_dict = self.__read_state_dict(criterion_object_name, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read criterion state dictionary from AWS S3: {ex}")
            raise ex  # re-raise

        try:
            constructor_args_dict = self.__read_state_dict(constructor_args_key, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read constructor arguments dictionary from Local Python Dict: {ex}")
            raise ex  # re-raise

        return model_state_dict, optimizer_state_dict, criterion_state_dict, constructor_args_dict

    def read_state_dicts(self, pointer: ModelPointer) -> tuple[
        Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        if pointer is None:
            raise ValueError("cannot read model using nil ModelPointer")

        model_name: str = pointer.large_object_name
        base_object_name: str = pointer.key

        model_object_name: str = os.path.join(base_object_name, "model.pt")
        optimizer_object_name: str = os.path.join(base_object_name, "optimizer.pt")
        criterion_object_name: str = os.path.join(base_object_name, "criterion.pt")
        constructor_args_key: str = os.path.join(base_object_name, "constructor_args.pt")

        try:
            model_state_dict = self.__read_state_dict(model_object_name, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read model state dictionary from AWS S3: {ex}")
            raise ex  # re-raise

        try:
            optimizer_state_dict = self.__read_state_dict(optimizer_object_name, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read optimizer state dictionary from AWS S3: {ex}")
            raise ex  # re-raise

        try:
            criterion_state_dict = self.__read_state_dict(criterion_object_name, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read criterion state dictionary from AWS S3: {ex}")
            raise ex  # re-raise

        try:
            constructor_args_dict = self.__read_state_dict(constructor_args_key, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read constructor arguments dictionary from Local Python Dict: {ex}")
            raise ex  # re-raise

        return model_state_dict, optimizer_state_dict, criterion_state_dict, constructor_args_dict

    def write_dataset(self, pointer: DatasetPointer):
        self.log.warning("write_dataset called for S3Checkpointer. This function doesn't do anything!")
        pass

    def __get_buffer_to_write(self, state_dict: Dict[str, Any], model_name: str) -> tuple[io.BytesIO, float]:
        buffer: io.BytesIO = io.BytesIO()

        try:
            torch.save(state_dict, buffer)
        except Exception as ex:
            self.log.error(f"Failed to save state of model \"{model_name}\" to io.BytesIO buffer because: {ex}")
            raise ex  # re-raise

        size_mb: float = buffer.getbuffer().nbytes / 1.0e6
        if buffer.getbuffer().nbytes > 512e6:
            self.log.error(f"Cannot write state of model \"{model_name}\" to Redis. "
                           f"Model state is larger than maximum size of 512 MB: {size_mb:,} MB.")
            raise ValueError("state dictionary buffer is too large (); maximum size is 512 MB")

        return buffer, size_mb

    async def __write_state_dict_async(self, object_name: str, state_dict: Dict[str, Any], model_name: str):
        """
        Write an individual state dictionary to Redis.

        :param object_name: the key at which the specified state dictionary is to be written.
        :param state_dict: the state dictionary to be written.
        :param model_name: the name of the model associated with the state dictionary that we've been instructed to write
        """
        buffer, size_mb = self.__get_buffer_to_write(state_dict, model_name)

        self.log.debug(f"Writing state dictionary associated with model \"{model_name}\" to AWS S3 at bucket/key "
                       f"\"{self._bucket_name + "/" + object_name}\". Model size: {size_mb:,} MB.")

        try:
            st: float = time.time()
            await self.upload_bytes_to_s3_async(buffer, object_name)
            et: float = time.time()
        except Exception as ex:
            self.log.error(
                f"Failed to write state of model \"{model_name}\" to AWS S3 at bucket/key \"{self._bucket_name + "/" + object_name}\" "
                f"(model size: {size_mb} MB) because: {ex}")
            raise ex  # re-raise

        self.log.debug(
            f"Successfully wrote state of model \"{model_name}\" to AWS S3 at bucket/key \"{self._bucket_name + "/" + object_name}\" "
            f"(model size: {size_mb} MB) in {et - st} seconds.")

    def __write_state_dict(self, object_name: str, state_dict: Dict[str, Any], model_name: str):
        """
        Write an individual state dictionary to Redis.

        :param object_name: the key at which the specified state dictionary is to be written.
        :param state_dict: the state dictionary to be written.
        :param model_name: the name of the model associated with the state dictionary that we've been instructed to write
        """
        buffer, size_mb = self.__get_buffer_to_write(state_dict, model_name)

        self.log.debug(f"Writing state dictionary associated with model \"{model_name}\" to AWS S3 at bucket/key "
                       f"\"{self._bucket_name + "/" + object_name}\". Model size: {size_mb:,} MB.")

        try:
            st: float = time.time()
            self.upload_bytes_to_s3(buffer, object_name)
            et: float = time.time()
        except Exception as ex:
            self.log.error(
                f"Failed to write state of model \"{model_name}\" to AWS S3 at bucket/key \"{self._bucket_name + "/" + object_name}\" "
                f"(model size: {size_mb} MB) because: {ex}")
            raise ex  # re-raise

        self.log.debug(
            f"Successfully wrote state of model \"{model_name}\" to AWS S3 at bucket/key \"{self._bucket_name + "/" + object_name}\" "
            f"(model size: {size_mb} MB) in {et - st} seconds.")

        self._num_objects_written += 1

    async def write_state_dicts_async(self, pointer: ModelPointer):
        if pointer is None:
            raise ValueError("cannot write model using nil ModelPointer")

        if pointer.model is None:
            self.log.error(f"Cannot model dataset \"{pointer.large_object_name}\"; invalid pointer.")
            raise ValueError(f"ModelPointer for model \"{pointer.large_object_name}\" does not have a valid pointer")

        model_name: str = pointer.large_object_name
        base_s3_key: str = pointer.key

        model_object_name: str = os.path.join(base_s3_key, "model.pt")
        await self.__write_state_dict_async(model_object_name, pointer.model.state_dict, model_name)

        optimizer_object_name: str = os.path.join(base_s3_key, "optimizer.pt")
        await self.__write_state_dict_async(optimizer_object_name, pointer.model.optimizer_state_dict, model_name)

        criterion_object_name: str = os.path.join(base_s3_key, "criterion.pt")
        await self.__write_state_dict_async(criterion_object_name, pointer.model.criterion_state_dict, model_name)

        constructor_state_key: str = os.path.join(base_s3_key, "constructor_args.pt")
        await self.__write_state_dict_async(constructor_state_key, pointer.model.constructor_args, model_name)

    def write_state_dicts(self, pointer: ModelPointer):
        if pointer is None:
            raise ValueError("cannot write model using nil ModelPointer")

        if pointer.model is None:
            self.log.error(f"Cannot model dataset \"{pointer.large_object_name}\"; invalid pointer.")
            raise ValueError(f"ModelPointer for model \"{pointer.large_object_name}\" does not have a valid pointer")

        model_name: str = pointer.large_object_name
        base_s3_key: str = pointer.key

        model_object_name: str = os.path.join(base_s3_key, "model.pt")
        self.__write_state_dict(
            object_name = model_object_name,
            state_dict =pointer.model.state_dict,
            model_name = model_name)

        optimizer_object_name: str = os.path.join(base_s3_key, "optimizer.pt")
        self.__write_state_dict(
            object_name = optimizer_object_name,
            state_dict =pointer.model.optimizer_state_dict,
            model_name = model_name)

        criterion_object_name: str = os.path.join(base_s3_key, "criterion.pt")
        self.__write_state_dict(
            object_name = criterion_object_name,
            state_dict =pointer.model.criterion_state_dict,
            model_name = model_name)

        constructor_state_key: str = os.path.join(base_s3_key, "constructor_args.pt")
        self.__write_state_dict(
            object_name = constructor_state_key,
            state_dict = pointer.model.constructor_args,
            model_name = model_name)

    def storage_name(self) -> str:
        return f"AWS S3"
