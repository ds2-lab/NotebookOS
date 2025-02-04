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

from distributed_notebook.sync.checkpointing.remote_checkpointer import RemoteCheckpointer
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
        self._redis_provider: RedisProvider = RedisProvider(
            host = host,
            port = port,
            db = db,
            password = password,
            additional_redis_args = additional_redis_args,
        )

        super().__init__(self._redis_provider)
