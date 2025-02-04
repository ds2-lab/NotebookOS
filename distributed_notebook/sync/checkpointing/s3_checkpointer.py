import io
import os
import time
from typing import Any, Dict, Optional

import aioboto3
import boto3
import torch

from distributed_notebook.sync.checkpointing.pointer import DatasetPointer, ModelPointer
from distributed_notebook.sync.checkpointing.checkpointer import Checkpointer
from distributed_notebook.sync.checkpointing.remote_checkpointer import RemoteCheckpointer
from distributed_notebook.sync.storage.s3_provider import S3Provider
from distributed_notebook.sync.storage.storage_provider import RemoteStorageProvider

DEFAULT_S3_BUCKET_NAME: str = "distributed-notebook-storage"


class S3Checkpointer(RemoteCheckpointer):
    def __init__(self, bucket_name: str = DEFAULT_S3_BUCKET_NAME):
        self._s3_provider: S3Provider = S3Provider(
            bucket_name = bucket_name
        )

        self._bucket_name: str = bucket_name

        super().__init__(self._s3_provider)