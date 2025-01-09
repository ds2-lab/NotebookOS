import io
import json
import asyncio
import os
import threading
import time

import torch

from distributed_notebook.datasets.custom_dataset import CustomDataset
from distributed_notebook.datasets.loader import load_dataset
from distributed_notebook.sync.checkpointing.remote_checkpointer import RemoteCheckpointer
from distributed_notebook.sync.checkpointing.pointer import DatasetPointer, ModelPointer

from typing import Optional, Any, Dict

class LocalCheckpointer(RemoteCheckpointer):
    """
    LocalCheckpointer is an implementation of RemoteCheckpointer that stores data locally in a dictionary.

    LocalCheckpointer is intended to be used only for unit tests.
    """

    def __init__(
            self,
    ):
        super().__init__()

        self._data: Dict[str, Any] = dict()
        self._async_lock: asyncio.Lock = asyncio.Lock()
        self._lock: threading.Lock = threading.Lock()

    @property
    def size(self)->int:
        return len(self._data)

    def __len__(self)->int:
        return self.size

    def read_dataset(self, pointer: DatasetPointer)->CustomDataset:
        if pointer is None:
            raise ValueError("cannot read dataset from nil DatasetPointer")

        key:str = pointer.key
        dataset_name: str = pointer.large_object_name

        self.log.debug(f"Reading description of dataset \"{dataset_name}\" from Local Python Dict at key \"{key}\" now...")

        st = time.time()
        val: Optional[str] = self._data.get(key, None)
        et = time.time()

        if val is None:
            self.log.error(f"Failed to read dataset \"{dataset_name}\". "
                           f"Nothing stored at key \"{key}\". "
                           f"Time elapsed: {et - st} seconds.")
            raise ValueError(f"failed to read dataset \"{dataset_name}\" "
                             f"because there is nothing stored at key \"{key}\"")

        self.log.debug(f"Successfully read description of dataset \"{dataset_name}\" from Local Python Dict. "
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


    def __read_state_dict(self, key: str, model_name: str)->Optional[Dict[str, Any]]:
        """
        Read a single state dictionary from local storage.
        :param key: the key at which the desired state dictionary is stored
        :param model_name: the name of the model associated with the state dictionary that we've been instructed to read
        :return: the desired state dictionary after being retrieved from local memory and deserialized using torch.load()
        """
        try:
            st: float = time.time()
            val: str|bytes|memoryview = self._data[key]
            et: float = time.time()
        except KeyError as ex:
            self.log.error(f"Failed to read state of model \"{model_name}\" from Local Python Dict at key \"{key}\" "
                           f"because there is no data stored at key \"{key}\"")
            raise ValueError(f"there is no data stored at key \"{key}\"")
        except Exception as ex:
            self.log.error(f"Failed to read state of model \"{model_name}\" from Local Python Dict at key \"{key}\" "
                           f"because: {ex}")
            raise ex # re-raise

        if val is None:
            self.log.error(f"Failed to read state of model \"{model_name}\" from Local Python Dict at key \"{key}\" "
                           f"because there was no value stored at that key.")
            raise ValueError(f"Failed to read state of model \"{model_name}\" from Local Python Dict at key \"{key}\" "
                             f"because there was no value stored at that key.")

        buffer: io.BytesIO = io.BytesIO(val)

        self.log.debug(f"Successfully read state of model \"{model_name}\" to Local Python Dict at key \"{key}\" "
                       f"(model size: {buffer.getbuffer().nbytes / 1.0e6} MB) in {et - st} seconds.")

        try:
            state_dict: Dict[str, Any] = torch.load(buffer)
        except Exception as ex:
            self.log.error(f"Failed to load state of model \"{model_name}\" from data retrieved from Local Python Dict "
                           f"at key \"{key}\" because: {ex}.")
            raise ex # re-raise

        return state_dict

    def storage_name(self)->str:
        return f"LocalInMemoryStorage"

    def __read_state_dicts(self, pointer: ModelPointer)->tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        if pointer is None:
            raise ValueError("cannot read model using nil ModelPointer")

        model_name:str = pointer.large_object_name
        base_key:str = pointer.key

        model_key: str = os.path.join(base_key, "model.pt")
        optimizer_key: str = os.path.join(base_key, "optimizer.pt")
        criterion_key: str = os.path.join(base_key, "criterion.pt")
        constructor_args_key: str = os.path.join(base_key, "constructor_args.pt")

        try:
            model_state_dict = self.__read_state_dict(model_key, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read model state dictionary from Local Python Dict: {ex}")
            raise ex # re-raise

        try:
            optimizer_state_dict = self.__read_state_dict(optimizer_key, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read optimizer state dictionary from Local Python Dict: {ex}")
            raise ex # re-raise

        try:
            criterion_state_dict = self.__read_state_dict(criterion_key, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read criterion state dictionary from Local Python Dict: {ex}")
            raise ex # re-raise

        try:
            constructor_args_dict = self.__read_state_dict(constructor_args_key, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read constructor arguments dictionary from Local Python Dict: {ex}")
            raise ex # re-raise

        return model_state_dict, optimizer_state_dict, criterion_state_dict, constructor_args_dict

    def read_state_dicts(self, pointer: ModelPointer)->tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        with self._lock:
            return self.__read_state_dicts(pointer)

    async def read_state_dicts_async(self, pointer: ModelPointer)->tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        async with self._async_lock:
            return self.__read_state_dicts(pointer)

    def __write_state_dict(self, key: str, state_dict: Dict[str, Any], model_name: str = ""):
        """
        Write an individual state dictionary to Local Python Dict.

        :param key: the key at which the specified state dictionary is to be written.
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
            self.log.error(f"Cannot write state of model \"{model_name}\" to Local Python Dict. "
                           f"Model state is larger than maximum size of 512 MB: {size_mb:,} MB.")
            raise ValueError("state dictionary buffer is too large (); maximum size is 512 MB")

        self.log.debug(f"Writing state dictionary associated with model \"{model_name}\" to Local Python Dict at key \"{key}\". "
                       f"Model size: {size_mb:,} MB.")

        self._data[key] = buffer.getbuffer()

        self.log.debug(f"Successfully wrote state of model \"{model_name}\" to Local Python Dictionary at key \"{key}\" "
                       f"(model size: {size_mb} MB).")

    def __write_state_dicts(self, pointer: ModelPointer):
        if pointer is None:
            raise ValueError("cannot write model using nil ModelPointer")

        if pointer.model is None:
            self.log.error(f"Cannot model dataset \"{pointer.large_object_name}\"; invalid pointer.")
            raise ValueError(f"ModelPointer for model \"{pointer.large_object_name}\" does not have a valid pointer")

        model_name:str = pointer.large_object_name
        base_key:str = pointer.key

        model_key: str = os.path.join(base_key, "model.pt")
        self.__write_state_dict(model_key, pointer.model.state_dict, model_name)

        optimizer_key: str = os.path.join(base_key, "optimizer.pt")
        self.__write_state_dict(optimizer_key, pointer.model.optimizer_state_dict, model_name)

        criterion_key: str = os.path.join(base_key, "criterion.pt")
        self.__write_state_dict(criterion_key, pointer.model.criterion_state_dict, model_name)

        constructor_state_key: str = os.path.join(base_key, "constructor_args.pt")
        self.__write_state_dict(constructor_state_key, pointer.model.constructor_args, model_name)

        pointer.wrote_model_state()

    async def write_state_dicts_async(self, pointer: ModelPointer):
        async with self._async_lock:
            self.__write_state_dicts(pointer)

    def write_state_dicts(self, pointer: ModelPointer):
        if pointer is None:
            raise ValueError("cannot write model using nil ModelPointer")

        if pointer.model is None:
            self.log.error(f"Cannot model dataset \"{pointer.large_object_name}\"; invalid pointer.")
            raise ValueError(f"ModelPointer for model \"{pointer.large_object_name}\" does not have a valid pointer")

        with self._lock:
            self.__write_state_dicts(pointer)