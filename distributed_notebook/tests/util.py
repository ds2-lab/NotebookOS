import os

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

def create_s3_bucket_if_not_exists(bucket_name: str, region: str = "us-east-1"):
    # Initialize the S3 client
    s3_client = boto3.client('s3', region_name=region)

    try:
        # Check if the bucket already exists
        existing_buckets = s3_client.list_buckets()
        for bucket in existing_buckets.get('Buckets', []):
            if bucket["Name"] == bucket_name:
                print(f"Bucket '{bucket_name}' already exists.")
                return

        # Create the bucket
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )

        print(f"Bucket '{bucket_name}' created successfully.")

    except (NoCredentialsError, PartialCredentialsError):
        print("AWS credentials not found. Please configure them.")
    except ClientError as e:
        print(f"Error: {e}")

def get_username():
    """
    Get and return the username of the current user.

    This should work on both Windows and Linux systems (with WSL/WSL2 treated as Linux).

    :return: the username of the current user.
    """
    if os.name == 'nt':
        try:
            return os.environ['USERNAME']
        except KeyError:
            return os.getlogin()
    else:
        try:
            return os.environ['USER']
        except KeyError:
            import pwd

            return pwd.getpwuid(os.getuid())[0]