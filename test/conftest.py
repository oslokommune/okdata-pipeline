import os

import boto3
import pytest
from moto import mock_aws


@pytest.fixture(scope="function")
def s3_client():
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture
def s3_bucket(s3_client):
    bucket_name = os.environ["BUCKET_NAME"]
    s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    return bucket_name
