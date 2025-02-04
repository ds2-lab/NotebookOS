import io
import json
import asyncio
import os
import sys
import time

import torch

from distributed_notebook.deep_learning.data.custom_dataset import CustomDataset
from distributed_notebook.deep_learning.data import load_dataset
from distributed_notebook.sync.checkpointing.checkpointer import Checkpointer
from distributed_notebook.sync.checkpointing.pointer import DatasetPointer, ModelPointer

from typing import Optional, Any, Dict

from distributed_notebook.sync.storage.storage_provider import RemoteStorageProvider


class RemoteCheckpointer(Checkpointer):
    def __init__(self, storage_provider: RemoteStorageProvider):
        super().__init__()

        self._lock = asyncio.Lock()
        self._storage_provider: RemoteStorageProvider = storage_provider

    @property
    def num_objects_written(self) -> int:
        """
        :return: the total, cumulative number of objects written to remote storage.
        """
        return self.storage_provider.num_objects_written

    @property
    def num_objects_read(self) -> int:
        """
        :return: the total, cumulative number of objects read from remote storage.
        """
        return self.storage_provider.num_objects_read

    @property
    def num_objects_deleted(self) -> int:
        """
        :return: the total, cumulative number of objects deleted from remote storage.
        """
        return self.storage_provider.num_objects_deleted

    @property
    def bytes_read(self) -> int:
        """
        :return: the total, cumulative number of bytes read from remote storage.
        """
        return self.storage_provider.bytes_read

    @property
    def bytes_written(self) -> int:
        """
        :return: the total, cumulative number of bytes written to remote storage.
        """
        return self.storage_provider.bytes_written

    @property
    def read_time(self) -> float:
        """
        :return: the total time spent reading data from remote storage in seconds.
        """
        return self.storage_provider.read_time

    @property
    def write_time(self) -> float:
        """
        :return: the total time spent writing data to remote storage in seconds.
        """
        return self.storage_provider.write_time

    @property
    def storage_provider(self)->RemoteStorageProvider:
        return self._storage_provider

    @property
    def storage_name(self)->str:
        return self.storage_provider.storage_name

    def read_dataset(self, pointer: DatasetPointer)->CustomDataset:
        if pointer is None:
            raise ValueError("cannot read dataset from nil DatasetPointer")

        key:str = pointer.key
        dataset_name: str = pointer.large_object_name

        self.log.debug(f"Reading description of dataset \"{dataset_name}\" from {self.storage_name} at key \"{key}\"")

        st = time.time()
        val: str|bytes|memoryview = self.storage_provider.read_value(key)
        et = time.time()

        if val is None:
            self.log.error(f"Failed to read dataset \"{dataset_name}\". "
                           f"Nothing stored at key \"{key}\". "
                           f"Time elapsed: {et - st} seconds.")
            raise ValueError(f"failed to read dataset \"{dataset_name}\" "
                             f"because there is nothing stored at key \"{key}\"")

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

    async def __read_state_dict_async(self, key: str, model_name: str)->Optional[Dict[str, Any]]:
        """
        Read a single state dictionary from remote storage.

        :param key: the key at which the desired state dictionary is stored
        :param model_name: the name of the model associated with the state dictionary that we've been instructed to read

        :return: the desired state dictionary after being retrieved from remote storage and deserialized using torch.load()
        """
        try:
            st: float = time.time()
            val: str|bytes|memoryview = await self.storage_provider.read_value_async(key)
            et: float = time.time()
            time_elapsed: float = et - st

            self.log.debug(f'Read {sys.getsizeof(val)} bytes from {self.storage_name} key "{key}" '
                           f'in {round(time_elapsed, 3):,} ms.')
        except Exception as ex:
            self.log.error(f"Failed to read state of model \"{model_name}\" from {self.storage_name} at key \"{key}\" "
                           f"because: {ex}")
            raise ex # re-raise

        if val is None:
            self.log.error(f"Failed to read state of model \"{model_name}\" from {self.storage_name} at key \"{key}\" "
                           f"because there was no value stored at that key.")
            raise ValueError(f"Failed to read state of model \"{model_name}\" from {self.storage_name} at key \"{key}\" "
                             f"because there was no value stored at that key.")

        buffer: io.BytesIO = io.BytesIO(val)

        self.log.debug(f"Successfully read state of model \"{model_name}\" to {self.storage_name} at key \"{key}\" "
                       f"(model size: {buffer.getbuffer().nbytes / 1.0e6} MB) in {et - st} seconds.")

        try:
            state_dict: Dict[str, Any] = torch.load(buffer)
        except Exception as ex:
            self.log.error(f"Failed to load state of model \"{model_name}\" from data retrieved from {self.storage_name} "
                           f"at key \"{key}\" because: {ex}.")
            raise ex # re-raise

        return state_dict


    def __read_state_dict(self, key: str, model_name: str)->Optional[Dict[str, Any]]:
        """
        Read a single state dictionary from remote storage.

        :param key: the key at which the desired state dictionary is stored
        :param model_name: the name of the model associated with the state dictionary that we've been instructed to read

        :return: the desired state dictionary after being retrieved from remote storage and deserialized using torch.load()
        """
        try:
            st: float = time.time()
            val: str|bytes|memoryview = self.storage_provider.read_value(key)
            et: float = time.time()
            time_elapsed: float = et - st

            self.log.debug(f'Read {sys.getsizeof(val)} bytes from {self.storage_name} key "{key}" '
                           f'in {round(time_elapsed, 3):,} ms.')
        except Exception as ex:
            self.log.error(f"Failed to read state of model \"{model_name}\" from {self.storage_name} at key \"{key}\" "
                           f"because: {ex}")
            raise ex # re-raise

        if val is None:
            self.log.error(f"Failed to read state of model \"{model_name}\" from {self.storage_name} at key \"{key}\" "
                           f"because there was no value stored at that key.")
            raise ValueError(f"Failed to read state of model \"{model_name}\" from {self.storage_name} at key \"{key}\" "
                             f"because there was no value stored at that key.")

        buffer: io.BytesIO = io.BytesIO(val)

        self.log.debug(f"Successfully read state of model \"{model_name}\" to {self.storage_name} at key \"{key}\" "
                       f"(model size: {buffer.getbuffer().nbytes / 1.0e6} MB) in {et - st} seconds.")

        try:
            state_dict: Dict[str, Any] = torch.load(buffer)
        except Exception as ex:
            self.log.error(f"Failed to load state of model \"{model_name}\" from data retrieved from {self.storage_name} "
                           f"at key \"{key}\" because: {ex}.")
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

    def __write_state_dict(self, key: str, state_dict: Dict[str, Any], model_name: str = ""):
        """
        Write an individual state dictionary to remote storage.

        :param key: the key at which the specified state dictionary is to be written.
        :param model_name: the name of the model associated with the state dictionary that we've been instructed to write
        """
        buffer, size_mb = self.__get_buffer_to_write(state_dict, model_name)

        self.log.debug(f"Writing state dictionary associated with model \"{model_name}\" to {self.storage_name} at key \"{key}\". "
                       f"Model size: {size_mb:,} MB.")

        try:
            self.storage_provider.write_value(key, buffer.getbuffer())
        except Exception as ex:
            self.log.error(f"Failed to write state of model \"{model_name}\" to {self.storage_name} at key \"{key}\" "
                           f"(model size: {size_mb} MB) because: {ex}")
            raise ex # re-raise

        self.log.debug(f"Successfully wrote state of model \"{model_name}\" to {self.storage_name} at key \"{key}\" "
                       f"(model size: {size_mb} MB).")

    async def __async_write_state_dict(self, key: str, state_dict: Dict[str, Any], model_name: str = ""):
        """
        Write an individual state dictionary to remote storage.

        :param key: the key at which the specified state dictionary is to be written.
        :param model_name: the name of the model associated with the state dictionary that we've been instructed to write
        """
        buffer, size_mb = self.__get_buffer_to_write(state_dict, model_name)

        self.log.debug(f"Writing state dictionary associated with model \"{model_name}\" to {self.storage_name} at key \"{key}\". "
                       f"Model size: {size_mb:,} MB.")

        try:
            st: float = time.time()
            await self.storage_provider.write_value_async(key, buffer.getbuffer())
            et: float = time.time()
            time_elapsed: float = et - st

            self.log.debug(f'{buffer.getbuffer().nbytes} bytes uploaded to {self.storage_name} at key "{key}" in '
                           f'{round(time_elapsed, 3):,}ms.')

        except Exception as ex:
            self.log.error(f"Failed to write state of model \"{model_name}\" to {self.storage_name} at key \"{key}\" "
                           f"(model size: {size_mb} MB) because: {ex}")
            raise ex # re-raise

        self.log.debug(f"Successfully wrote state of model \"{model_name}\" to {self.storage_name} at key \"{key}\" "
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