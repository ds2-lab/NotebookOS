from typing import Any, Optional, Dict

from distributed_notebook.sync.checkpointing.local_checkpointer import LocalCheckpointer
from distributed_notebook.sync.checkpointing.checkpointer import Checkpointer
from distributed_notebook.sync.checkpointing.remote_checkpointer import RemoteCheckpointer
from distributed_notebook.sync.storage.redis_provider import RedisProvider
from distributed_notebook.sync.storage.s3_provider import S3Provider

DEFAULT_S3_BUCKET_NAME: str = "distributed-notebook-storage"

def get_local_checkpointer()->LocalCheckpointer:
    return LocalCheckpointer()

def get_s3_checkpointer(bucket_name: str = DEFAULT_S3_BUCKET_NAME)->RemoteCheckpointer:
    s3_provider: S3Provider = S3Provider(bucket_name = bucket_name)
    return RemoteCheckpointer(s3_provider)

def get_redis_checkpointer(
        host:str = "",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        additional_redis_args: Optional[Dict[str, Any]] = None
)->RemoteCheckpointer:
    redis_provider: RedisProvider = RedisProvider(
        host = host,
        port = port,
        db = db,
        password = password,
        additional_redis_args = additional_redis_args,
    )

    return RemoteCheckpointer(redis_provider)

remote_checkpointer_factory: dict[str, Any] = {
    "redis": get_redis_checkpointer,
    "s3": get_s3_checkpointer,
    "local": LocalCheckpointer,
}

def get_checkpointer(remote_storage: str, **kwargs)->Checkpointer:
    if remote_storage is None:
        raise ValueError("remote storage cannot be null")

    if remote_storage.lower() not in remote_checkpointer_factory:
        raise ValueError(f"invalid or unsupported remote storage: \"{remote_storage}\"")

    return remote_checkpointer_factory[remote_storage.lower()](**kwargs)