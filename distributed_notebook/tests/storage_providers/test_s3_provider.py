import io

from distributed_notebook.sync.storage.s3_provider import S3Provider


def test_create():
    s3_provider: S3Provider = S3Provider(
        bucket_name = "distributed-notebook-storage",
        aws_region = "us-east-1"
    )

    assert s3_provider is not None
    assert isinstance(s3_provider, S3Provider)

def test_upload_and_download_file():
    s3_provider: S3Provider = S3Provider(
        bucket_name = "distributed-notebook-storage",
        aws_region = "us-east-1"
    )

    data: str = "Hello, S3! This is a string stored in an object."
    obj_name: str = "test_upload_and_download_file_data"

    success: bool = s3_provider.write_value(obj_name, data)
    assert success

    data: io.BytesIO = s3_provider.read_value(obj_name)
    print("Read data:", data.getvalue().decode("utf-8"))

    success = s3_provider.delete_value(obj_name)
    assert success

    assert data is not None

    assert s3_provider.num_objects_read == 1
    assert s3_provider.num_objects_written == 1
    assert s3_provider.num_objects_deleted == 1