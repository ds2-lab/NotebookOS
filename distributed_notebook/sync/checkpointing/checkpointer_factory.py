from typing import Any, Optional, Dict

from distributed_notebook.sync.checkpointing.checkpointer import Checkpointer
from distributed_notebook.sync.checkpointing.remote_checkpointer import RemoteCheckpointer
from distributed_notebook.sync.remote_storage.local_provider import LocalStorageProvider
from distributed_notebook.sync.remote_storage.redis_provider import RedisProvider
from distributed_notebook.sync.remote_storage.s3_provider import S3Provider, DEFAULT_AWS_S3_REGION, DEFAULT_S3_BUCKET_NAME

def get_local_checkpointer(**kwargs)->RemoteCheckpointer:
    local_provider: LocalStorageProvider = LocalStorageProvider()
    return RemoteCheckpointer(local_provider)

def get_s3_checkpointer(
        bucket_name: str = DEFAULT_S3_BUCKET_NAME,
        aws_region: str = DEFAULT_AWS_S3_REGION,
)->RemoteCheckpointer:
    s3_provider: S3Provider = S3Provider(bucket_name = bucket_name, aws_region = aws_region)
    return RemoteCheckpointer(s3_provider)

def get_redis_checkpointer(
        host:str = "",
        redis_port: int = 6379,
        redis_database: int = 0,
        redis_password: Optional[str] = None,
        additional_redis_args: Optional[Dict[str, Any]] = None,
        **kwargs,
)->RemoteCheckpointer:
    redis_provider: RedisProvider = RedisProvider(
        host = host,
        port = redis_port,
        db = redis_database,
        password = redis_password,
        additional_redis_args = additional_redis_args,
    )

    return RemoteCheckpointer(redis_provider)

remote_checkpointer_factory: dict[str, Any] = {
    "redis": get_redis_checkpointer,
    "s3": get_s3_checkpointer,
    "local": get_local_checkpointer,
}

def get_checkpointer(remote_storage_name: str, **kwargs)->Checkpointer:
    if remote_storage_name is None:
        raise ValueError("remote remote_storage cannot be null")

    if remote_storage_name.lower() not in remote_checkpointer_factory:
        raise ValueError(f"invalid or unsupported remote remote_storage: \"{remote_storage_name}\"")

    return remote_checkpointer_factory[remote_storage_name.lower()](**kwargs)