import io

from distributed_notebook.sync.remote_storage.local_provider import LocalStorageProvider

def test_create():
    local_provider: LocalStorageProvider = LocalStorageProvider()

    assert local_provider is not None
    assert isinstance(local_provider, LocalStorageProvider)

def test_upload_and_download_string():
    local_provider: LocalStorageProvider = LocalStorageProvider()

    data: str = "Hello, LocalStorageProvider! This is a string."
    obj_name: str = "test_upload_and_download_file_data"

    success: bool = local_provider.write_value(obj_name, data)
    assert success

    data: io.BytesIO = local_provider.read_value(obj_name)
    print("Read data:", data.getbuffer().tobytes().decode("utf-8"))

    success = local_provider.delete_value(obj_name)
    assert success

    assert data is not None

    assert local_provider.num_objects_read == 1
    assert local_provider.num_objects_written == 1
    assert local_provider.num_objects_deleted == 1