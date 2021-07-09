import boto3
from moto import mock_s3


def mock_aws_s3_client(bucket_name: str):
    # Must set region_name="us-east-1" in order for moto to create bucket.
    # Moto throws IllegalLocationConstraintException otherwise. Oyvind Nygard 2020-09-28
    mock_s3().start()
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=bucket_name)
    return s3_client
