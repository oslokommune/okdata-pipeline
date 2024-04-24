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


@pytest.fixture
def test_event():
    return {
        "execution_name": "boligpriser-UUID",
        "task": "write_cleaned",
        "payload": {
            "pipeline": {
                "id": "boligpriser",
                "task_config": {
                    "write_cleaned": {"output_stage": "cleaned"},
                    "write_processed": {"output_stage": "processed"},
                },
            },
            "output_dataset": {
                "id": "boligpriser",
                "version": "1",
                "edition": "20200120T133701",
                "s3_prefix": "%stage%/green/boligpriser/version=1/edition=20200120T133701/",
            },
            "step_data": {
                "s3_input_prefixes": {
                    "boligpriser": "raw/green/boligpriser/version=1/edition=20200120T133700/"
                },
                "status": "OK",
                "errors": [],
            },
        },
    }
