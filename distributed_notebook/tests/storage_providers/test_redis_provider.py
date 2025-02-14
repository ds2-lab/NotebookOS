import fakeredis
import pytest

from distributed_notebook.sync.remote_storage.redis_provider import RedisProvider

@pytest.fixture
def redis_client(request):
    redis_client = fakeredis.FakeRedis()
    return redis_client


@pytest.fixture
def async_redis_client(request):
    async_redis_client = fakeredis.FakeAsyncRedis()
    return async_redis_client

def test_create(redis_client, async_redis_client):
    redis_provider: RedisProvider = RedisProvider(
        redis_client = redis_client,
        async_redis_client = async_redis_client,
    )

    assert redis_provider is not None
    assert isinstance(redis_provider, RedisProvider)

def test_upload_and_download_string(redis_client, async_redis_client):
    redis_provider: RedisProvider = RedisProvider(
        redis_client = redis_client,
        async_redis_client = async_redis_client,
    )

    data: str = "Hello, Redis! This is a string."
    obj_name: str = "test_upload_and_download_file_data"

    success: bool = redis_provider.write_value(obj_name, data)
    assert success

    data_from_redis: str | bytes = redis_provider.read_value(obj_name)

    if isinstance(data_from_redis, bytes):
        data_from_redis = data_from_redis.decode()

    assert data_from_redis == data

    success = redis_provider.delete_value(obj_name)
    assert success

    assert data is not None

    assert redis_provider.num_objects_read == 1
    assert redis_provider.num_objects_written == 1
    assert redis_provider.num_objects_deleted == 1

def test_write_and_read_large_data(redis_client):
    redis_provider: RedisProvider = RedisProvider(
        redis_client = redis_client,
        async_redis_client = async_redis_client,
    )

    data: bytes = b'0' * 512 * 1024 * 1024 # 512 MB
    obj_name: str = "large_data"

    success: bool = redis_provider.write_value(obj_name, data)
    assert success

    data_from_redis: str | bytes = redis_provider.read_value(obj_name)

    # if isinstance(data_from_redis, bytes):
    #     data_from_redis = data_from_redis.decode()

    assert data_from_redis == data

    success = redis_provider.delete_value(obj_name)
    assert success

    assert data is not None

    assert redis_provider.num_objects_read == 4
    assert redis_provider.num_objects_written == 4
    assert redis_provider.num_objects_deleted == 1