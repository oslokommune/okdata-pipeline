import json
import okdata.pipeline.validators.json.s3_reader as s3_reader

test_prefix = "/a/path/to/somehwere"


def test_s3_reader_read_s3_data(mocker, s3_client, s3_bucket):
    test_data = {"foo": "bar", "bar": "foo"}

    s3_client.put_object(
        Bucket=s3_bucket,
        Key=f"{test_prefix}the-file.json",
        Body=json.dumps(test_data).encode("utf-8"),
    )

    mocker.patch("okdata.pipeline.validators.json.s3_reader.BUCKET", s3_bucket)
    mocker.patch("okdata.pipeline.validators.json.s3_reader.s3", s3_client)
    output = s3_reader.read_s3_data({"the-dataset-id": test_prefix})
    assert output == [test_data]
