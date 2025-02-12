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

    data: str | bytes = redis_provider.read_value(obj_name)
    print("Read data:", data.decode("utf-8"))

    success = redis_provider.delete_value(obj_name)
    assert success

    assert data is not None

    assert redis_provider.num_objects_read == 1
    assert redis_provider.num_objects_written == 1
    assert redis_provider.num_objects_deleted == 1