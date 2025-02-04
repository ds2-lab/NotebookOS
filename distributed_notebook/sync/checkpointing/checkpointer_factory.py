from typing import Any

from distributed_notebook.sync.checkpointing.hdfs_checkpointer import HdfsCheckpointer
from distributed_notebook.sync.checkpointing.local_checkpointer import LocalCheckpointer
from distributed_notebook.sync.checkpointing.redis_checkpointer import RedisCheckpointer
from distributed_notebook.sync.checkpointing.remote_checkpointer import RemoteCheckpointer
from distributed_notebook.sync.checkpointing.s3_checkpointer import S3Checkpointer

remote_checkpointer_factory: dict[str, Any] = {
    "redis": RedisCheckpointer,
    "s3": S3Checkpointer,
    "hdfs": HdfsCheckpointer,
    "local": LocalCheckpointer,
}

def get_remote_checkpointer(remote_storage: str, host: str)->RemoteCheckpointer:
    if remote_storage is None:
        raise ValueError("remote storage cannot be null")

    if remote_storage.lower() not in remote_checkpointer_factory:
        raise ValueError(f"invalid or unsupported remote storage: \"{remote_storage}\"")

    return remote_checkpointer_factory[remote_storage.lower()](host = host)