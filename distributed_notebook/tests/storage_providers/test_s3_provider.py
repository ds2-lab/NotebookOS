import io

from distributed_notebook.sync.storage.s3_provider import S3Provider, DEFAULT_S3_BUCKET_NAME

import pytest

import boto3

from moto import mock_aws

S3BucketName = DEFAULT_S3_BUCKET_NAME + "-unit-testing"

@pytest.fixture(autouse=True)
def moto_boto():
    """
    Ensures that we're mocking AWS S3.
    """
    with mock_aws():
        yield

@pytest.fixture
def s3_provider()->S3Provider:
    s3_provider: S3Provider = S3Provider(
        bucket_name=S3BucketName,
        aws_region="us-east-1"
    )

    return s3_provider

def test_create(s3_provider: S3Provider):
    assert s3_provider is not None
    assert isinstance(s3_provider, S3Provider)

def test_upload_and_download_string(s3_provider: S3Provider):
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=S3BucketName)

    data: str = "Hello, S3! This is a string."
    obj_name: str = "test_upload_and_download_file_data"

    success: bool = s3_provider.write_value(obj_name, data)
    assert success

    data: io.BytesIO = s3_provider.read_value(obj_name)
    assert isinstance(data, io.BytesIO)

    print("Read data:", data.getvalue().decode("utf-8"))

    success = s3_provider.delete_value(obj_name)
    assert success

    assert data is not None

    assert s3_provider.num_objects_read == 1
    assert s3_provider.num_objects_written == 1
    assert s3_provider.num_objects_deleted == 1