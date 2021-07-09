import okdata.pipeline.validators.json.s3_reader as s3_reader
from test.util import mock_aws_s3_client

test_bucket = "bucket"
test_prefix = "/a/path/to/somehwere"


def test_s3_reader_read_s3_data(mocker):
    s3_client_mock = mock_aws_s3_client(test_bucket)
    s3_client_mock.put_object(
        Bucket=test_bucket,
        Key=f"{test_prefix}the-file.json",
        Body="asdfasdf".encode("utf-8"),
    )

    mocker.patch("okdata.pipeline.validators.json.s3_reader.BUCKET", test_bucket)
    mocker.patch("okdata.pipeline.validators.json.s3_reader.s3", s3_client_mock)
    assert s3_reader.read_s3_data({"the-dataset-id": test_prefix}) == ["asdfasdf"]
