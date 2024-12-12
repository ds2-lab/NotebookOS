import io
import json
import time

import torch

from distributed_notebook.datasets.base import Dataset
from distributed_notebook.datasets.loader import load_dataset
from distributed_notebook.models.model import DeepLearningModel
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
            use_async: bool = False,
            additional_redis_args: Optional[dict] = None
    ):
        super().__init__()

        self._use_async: bool = use_async

        self._redis_password: str = password
        self._redis_db: int = db
        self._redis_port = port
        self._redis_host = host

        if additional_redis_args is None:
            additional_redis_args = dict()

        self._additional_redis_args: dict[str, Any] = additional_redis_args

        self.log.debug(f"Connecting to Redis server at {host}:{port} (db={db}).")
        if use_async:
            self._async_redis = async_redis.Redis(host = host, port = port, db = db, password = password, **additional_redis_args)
        else:
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

    def read_dataset(self, pointer: DatasetPointer)->Dataset:
        if pointer is None:
            raise ValueError("cannot read dataset from nil DatasetPointer")

        self.__ensure_redis()
        redis_key:str = pointer.key
        dataset_name: str = pointer.name

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
            dataset: Dataset = load_dataset(**dataset_description)
            return dataset
        except ValueError as ex:
            self.log.error(f"Failed to load dataset: {ex}")
            raise ValueError(f"failed to load dataset \"{dataset_name}\" because: {ex}")

    def read_model_state_dict(self, pointer: ModelPointer)->Dict[str, Any]:
        if pointer is None:
            raise ValueError("cannot read model using nil ModelPointer")

        self.__ensure_redis()

        model_name:str = pointer.name
        redis_key:str = pointer.key

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

        buffer: io.BytesIO = io.BytesIO(val)

        self.log.debug(f"Successfully read state of model \"{model_name}\" to Redis at key \"{redis_key}\" "
                       f"(model size: {buffer.getbuffer().nbytes} MB) in {et - st} seconds.")

        try:
            state_dict: Dict[str, Any] = torch.load(buffer)
        except Exception as ex:
            self.log.error(f"Failed to load state of model \"{model_name}\" from data retrieved from Redis "
                           f"at key \"{redis_key}\" because: {ex}.")
            raise ex # re-raise

        return state_dict

    def write_dataset(self, pointer: DatasetPointer):
        if pointer is None:
            raise ValueError("cannot write dataset using nil DatasetPointer")

        if pointer.dataset is None:
            self.log.error(f"Cannot write dataset \"{pointer.name}\"; invalid pointer.")
            raise ValueError(f"DatasetPointer for dataset \"{pointer.name}\" does not have a valid pointer")

        self.__ensure_redis()

        redis_key:str = pointer.key
        dataset_description: dict[str, str|int|bool] = pointer.dataset_description
        payload: str = json.dumps(dataset_description)

        self._redis.set(redis_key, payload)

    async def async_write_dataset(self, pointer: DatasetPointer):
        if pointer is None:
            raise ValueError("cannot write dataset using nil DatasetPointer")

        if pointer.dataset is None:
            self.log.error(f"Cannot write dataset \"{pointer.name}\"; invalid pointer.")
            raise ValueError(f"DatasetPointer for dataset \"{pointer.name}\" does not have a valid pointer")

        redis_key:str = pointer.key
        dataset_description: dict[str, str|int|bool] = pointer.dataset_description
        payload: str = json.dumps(dataset_description)

        await self._async_redis.set(redis_key, payload)

    def write_model_state_dict(self, pointer: ModelPointer):
        if pointer is None:
            raise ValueError("cannot write model using nil ModelPointer")

        if pointer.model is None:
            self.log.error(f"Cannot model dataset \"{pointer.name}\"; invalid pointer.")
            raise ValueError(f"ModelPointer for model \"{pointer.name}\" does not have a valid pointer")

        model: DeepLearningModel = pointer.model
        buffer: io.BytesIO = io.BytesIO()

        try:
            torch.save(pointer.model.state_dict, buffer)
        except Exception as ex:
            self.log.error(f"Failed to save state of model \"{model.name}\" to io.BytesIO buffer because: {ex}")
            raise ex # re-raise

        size_mb: float = buffer.getbuffer().nbytes / 1.0e6
        if buffer.getbuffer().nbytes > 512e6:
            self.log.error(f"Cannot write state of model \"{model.name}\" to Redis. "
                           f"Model state is larger than maximum size of 512 MB: {size_mb:,} MB.")

        redis_key:str = pointer.key

        self.log.debug(f"Writing state of model \"{model.name}\" to Redis at key \"{redis_key}\". "
                       f"Model size: {size_mb:,} MB.")

        try:
            st: float = time.time()
            self._redis.set(redis_key, buffer.getbuffer())
            et: float = time.time()
        except Exception as ex:
            self.log.error(f"Failed to write state of model \"{model.name}\" to Redis at key \"{redis_key}\" "
                           f"(model size: {size_mb} MB) because: {ex}")
            raise ex # re-raise

        self.log.debug(f"Successfully wrote state of model \"{model.name}\" to Redis at key \"{redis_key}\" "
                       f"(model size: {size_mb} MB) in {et - st} seconds.")