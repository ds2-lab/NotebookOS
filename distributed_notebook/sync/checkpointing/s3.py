from abc import ABC

from distributed_notebook.sync.checkpointing.remote_checkpointer import RemoteCheckpointer

import boto3
import os

class S3Checkpointer(RemoteCheckpointer, ABC):
    def __init__(self):
        super().__init__()

        self.s3_client = boto3.client('s3')

        raise ValueError("S3 is not yet supported.")

    def upload_file_to_s3(self, bucket_name, file_name, object_name=None):
        """
        Upload a file to an S3 bucket.

        :param bucket_name: Name of the S3 bucket
        :param file_name: File to upload
        :param object_name: S3 object name. If not specified, file_name is used
        :return: True if file was uploaded, else False
        """
        if object_name is None:
            object_name = os.path.basename(file_name)

        try:
            self.s3_client.upload_file(file_name, bucket_name, object_name)
            print(f"File {file_name} uploaded to {bucket_name}/{object_name}")
            return True
        except Exception as e:
            print(f"Error uploading file: {e}")
            return False

    def download_file_from_s3(self, bucket_name, object_name, file_name):
        """
        Download a file from an S3 bucket.

        :param bucket_name: Name of the S3 bucket
        :param object_name: S3 object name
        :param file_name: File name to save as locally
        :return: True if file was downloaded, else False
        """
        try:
            self.s3_client.download_file(bucket_name, object_name, file_name)
            print(f"File {object_name} from {bucket_name} downloaded as {file_name}")
            return True
        except Exception as e:
            print(f"Error downloading file: {e}")
            return False

    def storage_name(self)->str:
        return f"AWS S3"