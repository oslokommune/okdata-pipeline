from moto import mock_s3
import boto3
import pytest

import okdata.pipeline.validators.json.s3_reader as s3_reader

test_bucket = "bucket"
test_prefix = "/a/path/to/somehwere"


def test_s3_reader_read_s3_data(mocker, mock_s3_client):
    mocker.patch("okdata.pipeline.validators.json.s3_reader.BUCKET", test_bucket)
    mocker.patch("okdata.pipeline.validators.json.s3_reader.s3", mock_s3_client)
    assert s3_reader.read_s3_data({"the-dataset-id": test_prefix}) == "asdfasdf"


@pytest.fixture
def mock_s3_client():
    mock_s3().start()
    # Must set region_name="us-east-1" in order for moto to create bucket.
    # Moto throws IllegalLocationConstraintException otherwise.
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=test_bucket)

    s3_client.put_object(
        Bucket=test_bucket,
        Key=f"{test_prefix}the-file.json",
        Body="asdfasdf".encode("utf-8"),
    )

    return s3_client
