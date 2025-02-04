import io
import json
import asyncio
import os
import sys
import time

import torch

from distributed_notebook.deep_learning.data.custom_dataset import CustomDataset
from distributed_notebook.deep_learning.data import load_dataset
from distributed_notebook.sync.checkpointing.remote_checkpointer import RemoteCheckpointer
from distributed_notebook.sync.checkpointing.pointer import DatasetPointer, ModelPointer

from typing import Optional, Any, Dict

from distributed_notebook.sync.storage.redis_provider import RedisProvider
from distributed_notebook.sync.storage.storage_provider import RemoteStorageProvider


class RedisCheckpointer(RemoteCheckpointer):
    def __init__(
            self,
            host:str = "",
            port: int = 6379,
            db: int = 0,
            password: Optional[str] = None,
            additional_redis_args: Optional[dict] = None
    ):
        super().__init__()

        self._lock = asyncio.Lock()

        self._redis_provider: RedisProvider = RedisProvider(
            host = host,
            port = port,
            db = db,
            password = password,
            additional_redis_args = additional_redis_args,
        )

    @property
    def storage_provider(self)->RemoteStorageProvider:
        return self._redis_provider

    @property
    def storage_name(self)->str:
        return self.storage_provider.storage_name

    def read_dataset(self, pointer: DatasetPointer)->CustomDataset:
        if pointer is None:
            raise ValueError("cannot read dataset from nil DatasetPointer")


        redis_key:str = pointer.key
        dataset_name: str = pointer.large_object_name

        self.log.debug(f"Reading description of dataset \"{dataset_name}\" from redis at key \"{redis_key}\" now...")

        st = time.time()
        val: str|bytes|memoryview = self.storage_provider.read_value(redis_key)
        et = time.time()

        if val is None:
            self.log.error(f"Failed to read dataset \"{dataset_name}\". "
                           f"Nothing stored at redis key \"{redis_key}\". "
                           f"Time elapsed: {et - st} seconds.")
            raise ValueError(f"failed to read dataset \"{dataset_name}\" "
                             f"because there is nothing stored at redis key \"{redis_key}\"")

        self.log.debug(f"Successfully read description of dataset \"{dataset_name}\" from {self.storage_name}. "
                       f"Time elapsed: {et - st} seconds.")

        try:
            dataset_description: dict[str, str|int|bool] = json.loads(val)
        except json.JSONDecodeError as ex:
            self.log.error(f"Failed to decode dataset description via JSON due to JSONDecodeError: {ex}")
            raise ValueError(f"failed to load dataset \"{dataset_name}\" "
                             f"because there was a JSONDecodeError while decoding the dataset's description: {ex}")
        except TypeError as ex:
            self.log.error(f"Failed to decode dataset description via JSON due to TypeError: {ex}")
            raise ValueError(f"failed to load dataset \"{dataset_name}\" "
                             f"because there was a TypeError while decoding the dataset's description: {ex}")

        try:
            dataset: CustomDataset = load_dataset(**dataset_description)
            return dataset
        except ValueError as ex:
            self.log.error(f"Failed to load dataset: {ex}")
            raise ValueError(f"failed to load dataset \"{dataset_name}\" because: {ex}")

    async def __read_state_dict_async(self, redis_key: str, model_name: str)->Optional[Dict[str, Any]]:
        """
        Read a single state dictionary from remote storage.

        :param redis_key: the key at which the desired state dictionary is stored
        :param model_name: the name of the model associated with the state dictionary that we've been instructed to read

        :return: the desired state dictionary after being retrieved from redis and deserialized using torch.load()
        """
        try:
            st: float = time.time()
            val: str|bytes|memoryview = await self.storage_provider.read_value_async(redis_key)
            et: float = time.time()
            time_elapsed: float = et - st

            self.log.debug(f'Read {sys.getsizeof(val)} bytes from {self.storage_name} key "{redis_key}" '
                           f'in {round(time_elapsed, 3):,} ms.')

            self._read_time += time_elapsed
            self._num_objects_read += 1
        except Exception as ex:
            self.log.error(f"Failed to read state of model \"{model_name}\" from {self.storage_name} at key \"{redis_key}\" "
                           f"because: {ex}")
            raise ex # re-raise

        if val is None:
            self.log.error(f"Failed to read state of model \"{model_name}\" from {self.storage_name} at key \"{redis_key}\" "
                           f"because there was no value stored at that key.")
            raise ValueError(f"Failed to read state of model \"{model_name}\" from {self.storage_name} at key \"{redis_key}\" "
                             f"because there was no value stored at that key.")

        buffer: io.BytesIO = io.BytesIO(val)

        self.log.debug(f"Successfully read state of model \"{model_name}\" to {self.storage_name} at key \"{redis_key}\" "
                       f"(model size: {buffer.getbuffer().nbytes / 1.0e6} MB) in {et - st} seconds.")

        try:
            state_dict: Dict[str, Any] = torch.load(buffer)
        except Exception as ex:
            self.log.error(f"Failed to load state of model \"{model_name}\" from data retrieved from {self.storage_name} "
                           f"at key \"{redis_key}\" because: {ex}.")
            raise ex # re-raise

        return state_dict


    def __read_state_dict(self, redis_key: str, model_name: str)->Optional[Dict[str, Any]]:
        """
        Read a single state dictionary from remote storage.

        :param redis_key: the key at which the desired state dictionary is stored
        :param model_name: the name of the model associated with the state dictionary that we've been instructed to read

        :return: the desired state dictionary after being retrieved from redis and deserialized using torch.load()
        """
        try:
            st: float = time.time()
            val: str|bytes|memoryview = self.storage_provider.read_value(redis_key)
            et: float = time.time()
            time_elapsed: float = et - st

            self.log.debug(f'Read {sys.getsizeof(val)} bytes from {self.storage_name} key "{redis_key}" '
                           f'in {round(time_elapsed, 3):,} ms.')

            self._read_time += time_elapsed
            self._num_objects_read += 1
        except Exception as ex:
            self.log.error(f"Failed to read state of model \"{model_name}\" from {self.storage_name} at key \"{redis_key}\" "
                           f"because: {ex}")
            raise ex # re-raise

        if val is None:
            self.log.error(f"Failed to read state of model \"{model_name}\" from {self.storage_name} at key \"{redis_key}\" "
                           f"because there was no value stored at that key.")
            raise ValueError(f"Failed to read state of model \"{model_name}\" from {self.storage_name} at key \"{redis_key}\" "
                             f"because there was no value stored at that key.")

        buffer: io.BytesIO = io.BytesIO(val)

        self.log.debug(f"Successfully read state of model \"{model_name}\" to {self.storage_name} at key \"{redis_key}\" "
                       f"(model size: {buffer.getbuffer().nbytes / 1.0e6} MB) in {et - st} seconds.")

        try:
            state_dict: Dict[str, Any] = torch.load(buffer)
        except Exception as ex:
            self.log.error(f"Failed to load state of model \"{model_name}\" from data retrieved from {self.storage_name} "
                           f"at key \"{redis_key}\" because: {ex}.")
            raise ex # re-raise

        return state_dict

    def read_state_dicts(self, pointer: ModelPointer)->tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        if pointer is None:
            raise ValueError("cannot read model using nil ModelPointer")

        model_name:str = pointer.large_object_name
        base_redis_key:str = pointer.key

        model_redis_key: str = os.path.join(base_redis_key, "model.pt")
        optimizer_redis_key: str = os.path.join(base_redis_key, "optimizer.pt")
        criterion_redis_key: str = os.path.join(base_redis_key, "criterion.pt")
        constructor_args_key: str = os.path.join(base_redis_key, "constructor_args.pt")

        try:
            model_state_dict = self.__read_state_dict(model_redis_key, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read model state dictionary from {self.storage_name}: {ex}")
            raise ex # re-raise

        try:
            optimizer_state_dict = self.__read_state_dict(optimizer_redis_key, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read optimizer state dictionary from {self.storage_name}: {ex}")
            raise ex # re-raise

        try:
            criterion_state_dict = self.__read_state_dict(criterion_redis_key, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read criterion state dictionary from {self.storage_name}: {ex}")
            raise ex # re-raise

        try:
            constructor_args_dict = self.__read_state_dict(constructor_args_key, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read constructor arguments dictionary from Local Python Dict: {ex}")
            raise ex # re-raise

        return model_state_dict, optimizer_state_dict, criterion_state_dict, constructor_args_dict

    async def read_state_dicts_async(self, pointer: ModelPointer)->tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        async with self._lock:
            if pointer is None:
                raise ValueError("cannot read model using nil ModelPointer")

            model_name:str = pointer.large_object_name
            base_redis_key:str = pointer.key

            model_redis_key: str = os.path.join(base_redis_key, "model.pt")
            optimizer_redis_key: str = os.path.join(base_redis_key, "optimizer.pt")
            criterion_redis_key: str = os.path.join(base_redis_key, "criterion.pt")
            constructor_args_key: str = os.path.join(base_redis_key, "constructor_args.pt")

            try:
                model_state_dict = await self.__read_state_dict_async(model_redis_key, model_name)
            except Exception as ex:
                self.log.error(f"Failed to read model state dictionary from {self.storage_name}: {ex}")
                raise ex # re-raise

            try:
                optimizer_state_dict = await self.__read_state_dict_async(optimizer_redis_key, model_name)
            except Exception as ex:
                self.log.error(f"Failed to read optimizer state dictionary from {self.storage_name}: {ex}")
                raise ex # re-raise

            try:
                criterion_state_dict = await self.__read_state_dict_async(criterion_redis_key, model_name)
            except Exception as ex:
                self.log.error(f"Failed to read criterion state dictionary from {self.storage_name}: {ex}")
                raise ex # re-raise

            try:
                constructor_args_dict = self.__read_state_dict(constructor_args_key, model_name)
            except Exception as ex:
                self.log.error(f"Failed to read constructor arguments dictionary from Local Python Dict: {ex}")
                raise ex # re-raise

            return model_state_dict, optimizer_state_dict, criterion_state_dict, constructor_args_dict


    def __write_state_dict(self, redis_key: str, state_dict: Dict[str, Any], model_name: str = ""):
        """
        Write an individual state dictionary to remote storage.

        :param redis_key: the key at which the specified state dictionary is to be written.
        :param model_name: the name of the model associated with the state dictionary that we've been instructed to write
        """
        buffer: io.BytesIO = io.BytesIO()

        try:
            torch.save(state_dict, buffer)
        except Exception as ex:
            self.log.error(f"Failed to save state of model \"{model_name}\" to io.BytesIO buffer because: {ex}")
            raise ex # re-raise

        size_mb: float = buffer.getbuffer().nbytes / 1.0e6
        if buffer.getbuffer().nbytes > 512e6:
            self.log.error(f"Cannot write state of model \"{model_name}\" to {self.storage_name}. "
                           f"Model state is larger than maximum size of 512 MB: {size_mb:,} MB.")
            raise ValueError("state dictionary buffer is too large (); maximum size is 512 MB")

        self.log.debug(f"Writing state dictionary associated with model \"{model_name}\" to {self.storage_name} at key \"{redis_key}\". "
                       f"Model size: {size_mb:,} MB.")

        try:
            self.storage_provider.write_value(redis_key, buffer.getbuffer())
        except Exception as ex:
            self.log.error(f"Failed to write state of model \"{model_name}\" to {self.storage_name} at key \"{redis_key}\" "
                           f"(model size: {size_mb} MB) because: {ex}")
            raise ex # re-raise

        self.log.debug(f"Successfully wrote state of model \"{model_name}\" to {self.storage_name} at key \"{redis_key}\" "
                       f"(model size: {size_mb} MB).")

    async def __async_write_state_dict(self, redis_key: str, state_dict: Dict[str, Any], model_name: str = ""):
        """
        Write an individual state dictionary to remote storage..

        :param redis_key: the key at which the specified state dictionary is to be written.
        :param model_name: the name of the model associated with the state dictionary that we've been instructed to write
        """
        buffer: io.BytesIO = io.BytesIO()

        try:
            torch.save(state_dict, buffer)
        except Exception as ex:
            self.log.error(f"Failed to save state of model \"{model_name}\" to io.BytesIO buffer because: {ex}")
            raise ex # re-raise

        size_mb: float = buffer.getbuffer().nbytes / 1.0e6
        if buffer.getbuffer().nbytes > 512e6:
            self.log.error(f"Cannot write state of model \"{model_name}\" to {self.storage_name}. "
                           f"Model state is larger than maximum size of 512 MB: {size_mb:,} MB.")
            raise ValueError("state dictionary buffer is too large (); maximum size is 512 MB")

        self.log.debug(f"Writing state dictionary associated with model \"{model_name}\" to {self.storage_name} at key \"{redis_key}\". "
                       f"Model size: {size_mb:,} MB.")

        try:
            st: float = time.time()
            await self.storage_provider.write_value_async(redis_key, buffer.getbuffer())
            et: float = time.time()
            time_elapsed: float = et - st

            self.log.debug(f'{buffer.getbuffer().nbytes} bytes uploaded to {self.storage_name} at key "{redis_key}" in '
                           f'{round(time_elapsed, 3):,}ms.')

            self._num_objects_written += 1
            self._write_time += time_elapsed
        except Exception as ex:
            self.log.error(f"Failed to write state of model \"{model_name}\" to {self.storage_name} at key \"{redis_key}\" "
                           f"(model size: {size_mb} MB) because: {ex}")
            raise ex # re-raise

        self.log.debug(f"Successfully wrote state of model \"{model_name}\" to {self.storage_name} at key \"{redis_key}\" "
                       f"(model size: {size_mb} MB) in {et - st} seconds.")

    async def write_state_dicts_async(self, pointer: ModelPointer):
        async with self._lock:
            if pointer is None:
                raise ValueError("cannot write model using nil ModelPointer")

            if pointer.model is None:
                self.log.error(f"Cannot model dataset \"{pointer.large_object_name}\"; invalid pointer.")
                raise ValueError(f"ModelPointer for model \"{pointer.large_object_name}\" does not have a valid pointer")

            model_name:str = pointer.large_object_name
            base_redis_key:str = pointer.key

            model_redis_key: str = os.path.join(base_redis_key, "model.pt")
            await self.__async_write_state_dict(model_redis_key, pointer.model.state_dict, model_name)

            optimizer_redis_key: str = os.path.join(base_redis_key, "optimizer.pt")
            await self.__async_write_state_dict(optimizer_redis_key, pointer.model.optimizer_state_dict, model_name)

            criterion_redis_key: str = os.path.join(base_redis_key, "criterion.pt")
            await self.__async_write_state_dict(criterion_redis_key, pointer.model.criterion_state_dict, model_name)

            constructor_state_key: str = os.path.join(base_redis_key, "constructor_args.pt")
            await self.__async_write_state_dict(constructor_state_key, pointer.model.constructor_args, model_name)

            pointer.wrote_model_state()

    def write_state_dicts(self, pointer: ModelPointer):
        if pointer is None:
            raise ValueError("cannot write model using nil ModelPointer")

        if pointer.model is None:
            self.log.error(f"Cannot model dataset \"{pointer.large_object_name}\"; invalid pointer.")
            raise ValueError(f"ModelPointer for model \"{pointer.large_object_name}\" does not have a valid pointer")

        model_name:str = pointer.large_object_name
        base_redis_key:str = pointer.key

        model_redis_key: str = os.path.join(base_redis_key, "model.pt")
        self.__write_state_dict(model_redis_key, pointer.model.state_dict, model_name)

        optimizer_redis_key: str = os.path.join(base_redis_key, "optimizer.pt")
        self.__write_state_dict(optimizer_redis_key, pointer.model.optimizer_state_dict, model_name)

        criterion_redis_key: str = os.path.join(base_redis_key, "criterion.pt")
        self.__write_state_dict(criterion_redis_key, pointer.model.criterion_state_dict, model_name)

        constructor_state_key: str = os.path.join(base_redis_key, "constructor_args.pt")
        self.__write_state_dict(constructor_state_key, pointer.model.constructor_args, model_name)

        pointer.wrote_model_state()