import io
import json
import asyncio
import os
import time

import torch

from distributed_notebook.deep_learning.datasets.custom_dataset import CustomDataset
from distributed_notebook.deep_learning.datasets.loader import load_dataset
from distributed_notebook.sync.checkpointing.remote_checkpointer import RemoteCheckpointer
from distributed_notebook.sync.checkpointing.pointer import DatasetPointer, ModelPointer

from typing import Optional, Any, Dict

import redis
import redis.asyncio as async_redis

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

        if ':' in host:
            host_orig:str = host
            idx = host_orig.index(':')
            host = host_orig[:idx]

            try:
                port = int(host_orig[idx+1:]) # +1 because idx is the index of the ':', not the beginning of the port
            except IndexError:
                # Apparently there's nothing after the colon, so we'll use the default of 6379 (or whatever was passed).
                pass

        self._redis_password: str = password
        self._redis_db: int = db
        self._redis_port = port
        self._redis_host = host

        if additional_redis_args is None:
            additional_redis_args = dict()

        self._additional_redis_args: dict[str, Any] = additional_redis_args
        self._lock = asyncio.Lock()

        self.log.debug(f"Connecting to Redis server at {host}:{port} (db={db}).")
        self._async_redis = async_redis.Redis(host = host, port = port, db = db, password = password, **additional_redis_args)
        self._redis = redis.Redis(host = host, port = port, db = db, password = password, **additional_redis_args)

    def __ensure_async_redis(self)->bool:
        """
        Ensure the target RedisCheckpointer object has created its async Redis client.

        :return: true if the async Redis client already existed, false if the async Redis client did not already exist.
        """
        if getattr(self, "_async_redis") is None:
            self._async_redis = async_redis.Redis(
                host = self._redis_host,
                port = self._redis_port,
                db = self._redis_db,
                password = self._redis_password,
                **self._additional_redis_args
            )
            return False
        else:
            return True

    def __ensure_redis(self):
        """
        Ensure the target RedisCheckpointer object has created its synchronous Redis client.

        :return: true if the synchronous Redis client already existed,
                 false if the synchronous Redis client did not already exist.
        """
        if getattr(self, "_redis") is None:
            self._redis = redis.Redis(
                host = self._redis_host,
                port = self._redis_port,
                db = self._redis_db,
                password = self._redis_password,
                **self._additional_redis_args
            )

    def read_dataset(self, pointer: DatasetPointer)->CustomDataset:
        if pointer is None:
            raise ValueError("cannot read dataset from nil DatasetPointer")

        self.__ensure_redis()
        redis_key:str = pointer.key
        dataset_name: str = pointer.large_object_name

        self.log.debug(f"Reading description of dataset \"{dataset_name}\" from redis at key \"{redis_key}\" now...")

        st = time.time()
        val: Optional[str] = self._redis.get(redis_key)
        et = time.time()

        if val is None:
            self.log.error(f"Failed to read dataset \"{dataset_name}\". "
                           f"Nothing stored at redis key \"{redis_key}\". "
                           f"Time elapsed: {et - st} seconds.")
            raise ValueError(f"failed to read dataset \"{dataset_name}\" "
                             f"because there is nothing stored at redis key \"{redis_key}\"")

        self.log.debug(f"Successfully read description of dataset \"{dataset_name}\" from Redis. "
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
        Read a single state dictionary from Redis.
        :param redis_key: the key at which the desired state dictionary is stored
        :param model_name: the name of the model associated with the state dictionary that we've been instructed to read
        :return: the desired state dictionary after being retrieved from redis and deserialized using torch.load()
        """
        try:
            st: float = time.time()
            val: str|bytes|memoryview = await self._async_redis.get(redis_key)
            et: float = time.time()
        except Exception as ex:
            self.log.error(f"Failed to read state of model \"{model_name}\" from Redis at key \"{redis_key}\" "
                           f"because: {ex}")
            raise ex # re-raise

        if val is None:
            self.log.error(f"Failed to read state of model \"{model_name}\" from Redis at key \"{redis_key}\" "
                           f"because there was no value stored at that key.")
            raise ValueError(f"Failed to read state of model \"{model_name}\" from Redis at key \"{redis_key}\" "
                             f"because there was no value stored at that key.")

        buffer: io.BytesIO = io.BytesIO(val)

        self.log.debug(f"Successfully read state of model \"{model_name}\" to Redis at key \"{redis_key}\" "
                       f"(model size: {buffer.getbuffer().nbytes / 1.0e6} MB) in {et - st} seconds.")

        try:
            state_dict: Dict[str, Any] = torch.load(buffer)
        except Exception as ex:
            self.log.error(f"Failed to load state of model \"{model_name}\" from data retrieved from Redis "
                           f"at key \"{redis_key}\" because: {ex}.")
            raise ex # re-raise

        return state_dict


    def __read_state_dict(self, redis_key: str, model_name: str)->Optional[Dict[str, Any]]:
        """
        Read a single state dictionary from Redis.
        :param redis_key: the key at which the desired state dictionary is stored
        :param model_name: the name of the model associated with the state dictionary that we've been instructed to read
        :return: the desired state dictionary after being retrieved from redis and deserialized using torch.load()
        """
        try:
            st: float = time.time()
            val: str|bytes|memoryview = self._redis.get(redis_key)
            et: float = time.time()
        except Exception as ex:
            self.log.error(f"Failed to read state of model \"{model_name}\" from Redis at key \"{redis_key}\" "
                           f"because: {ex}")
            raise ex # re-raise

        if val is None:
            self.log.error(f"Failed to read state of model \"{model_name}\" from Redis at key \"{redis_key}\" "
                           f"because there was no value stored at that key.")
            raise ValueError(f"Failed to read state of model \"{model_name}\" from Redis at key \"{redis_key}\" "
                             f"because there was no value stored at that key.")

        buffer: io.BytesIO = io.BytesIO(val)

        self.log.debug(f"Successfully read state of model \"{model_name}\" to Redis at key \"{redis_key}\" "
                       f"(model size: {buffer.getbuffer().nbytes / 1.0e6} MB) in {et - st} seconds.")

        try:
            state_dict: Dict[str, Any] = torch.load(buffer)
        except Exception as ex:
            self.log.error(f"Failed to load state of model \"{model_name}\" from data retrieved from Redis "
                           f"at key \"{redis_key}\" because: {ex}.")
            raise ex # re-raise

        return state_dict

    def storage_name(self)->str:
        return f"Redis({self._redis_host}:{self._redis_port},db={self._redis_db})"

    def read_state_dicts(self, pointer: ModelPointer)->tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        if pointer is None:
            raise ValueError("cannot read model using nil ModelPointer")

        self.__ensure_redis()

        model_name:str = pointer.large_object_name
        base_redis_key:str = pointer.key

        model_redis_key: str = os.path.join(base_redis_key, "model.pt")
        optimizer_redis_key: str = os.path.join(base_redis_key, "optimizer.pt")
        criterion_redis_key: str = os.path.join(base_redis_key, "criterion.pt")
        constructor_args_key: str = os.path.join(base_redis_key, "constructor_args.pt")

        try:
            model_state_dict = self.__read_state_dict(model_redis_key, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read model state dictionary from Redis: {ex}")
            raise ex # re-raise

        try:
            optimizer_state_dict = self.__read_state_dict(optimizer_redis_key, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read optimizer state dictionary from Redis: {ex}")
            raise ex # re-raise

        try:
            criterion_state_dict = self.__read_state_dict(criterion_redis_key, model_name)
        except Exception as ex:
            self.log.error(f"Failed to read criterion state dictionary from Redis: {ex}")
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

            self.__ensure_async_redis()

            model_name:str = pointer.large_object_name
            base_redis_key:str = pointer.key

            model_redis_key: str = os.path.join(base_redis_key, "model.pt")
            optimizer_redis_key: str = os.path.join(base_redis_key, "optimizer.pt")
            criterion_redis_key: str = os.path.join(base_redis_key, "criterion.pt")
            constructor_args_key: str = os.path.join(base_redis_key, "constructor_args.pt")

            try:
                model_state_dict = await self.__read_state_dict_async(model_redis_key, model_name)
            except Exception as ex:
                self.log.error(f"Failed to read model state dictionary from Redis: {ex}")
                raise ex # re-raise

            try:
                optimizer_state_dict = await self.__read_state_dict_async(optimizer_redis_key, model_name)
            except Exception as ex:
                self.log.error(f"Failed to read optimizer state dictionary from Redis: {ex}")
                raise ex # re-raise

            try:
                criterion_state_dict = await self.__read_state_dict_async(criterion_redis_key, model_name)
            except Exception as ex:
                self.log.error(f"Failed to read criterion state dictionary from Redis: {ex}")
                raise ex # re-raise

            try:
                constructor_args_dict = self.__read_state_dict(constructor_args_key, model_name)
            except Exception as ex:
                self.log.error(f"Failed to read constructor arguments dictionary from Local Python Dict: {ex}")
                raise ex # re-raise

            return model_state_dict, optimizer_state_dict, criterion_state_dict, constructor_args_dict


    def __write_state_dict(self, redis_key: str, state_dict: Dict[str, Any], model_name: str = ""):
        """
        Write an individual state dictionary to Redis.

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
            self.log.error(f"Cannot write state of model \"{model_name}\" to Redis. "
                           f"Model state is larger than maximum size of 512 MB: {size_mb:,} MB.")
            raise ValueError("state dictionary buffer is too large (); maximum size is 512 MB")

        self.log.debug(f"Writing state dictionary associated with model \"{model_name}\" to Redis at key \"{redis_key}\". "
                       f"Model size: {size_mb:,} MB.")

        try:
            st: float = time.time()
            self._redis.set(redis_key, buffer.getbuffer())
            et: float = time.time()
        except Exception as ex:
            self.log.error(f"Failed to write state of model \"{model_name}\" to Redis at key \"{redis_key}\" "
                           f"(model size: {size_mb} MB) because: {ex}")
            raise ex # re-raise

        self.log.debug(f"Successfully wrote state of model \"{model_name}\" to Redis at key \"{redis_key}\" "
                       f"(model size: {size_mb} MB) in {et - st} seconds.")

    async def __async_write_state_dict(self, redis_key: str, state_dict: Dict[str, Any], model_name: str = ""):
        """
        Write an individual state dictionary to Redis.

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
            self.log.error(f"Cannot write state of model \"{model_name}\" to Redis. "
                           f"Model state is larger than maximum size of 512 MB: {size_mb:,} MB.")
            raise ValueError("state dictionary buffer is too large (); maximum size is 512 MB")

        self.log.debug(f"Writing state dictionary associated with model \"{model_name}\" to Redis at key \"{redis_key}\". "
                       f"Model size: {size_mb:,} MB.")

        try:
            st: float = time.time()
            await self._async_redis.set(redis_key, buffer.getbuffer())
            et: float = time.time()
        except Exception as ex:
            self.log.error(f"Failed to write state of model \"{model_name}\" to Redis at key \"{redis_key}\" "
                           f"(model size: {size_mb} MB) because: {ex}")
            raise ex # re-raise

        self.log.debug(f"Successfully wrote state of model \"{model_name}\" to Redis at key \"{redis_key}\" "
                       f"(model size: {size_mb} MB) in {et - st} seconds.")

    async def write_state_dicts_async(self, pointer: ModelPointer):
        async with self._lock:
            if pointer is None:
                raise ValueError("cannot write model using nil ModelPointer")

            self.__ensure_async_redis()

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