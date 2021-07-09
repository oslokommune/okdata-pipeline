import pytest
from botocore.stub import Stubber

import test.writers.s3.test_data as test_data
from okdata.pipeline.writers.s3.exceptions import IncompleteTransaction
from okdata.pipeline.writers.s3.services import S3Service
from test.util import mock_aws_s3_client

file_content_1 = "nfjanfdafmkadmfa"
file_content_2 = "djnfjandjfnsekjg"

test_files = [
    {"filename": test_data.file_name_1, "content": file_content_1.encode("utf-8")},
    {"filename": test_data.file_name_2, "content": file_content_2.encode("utf-8")},
]


def test_copy(mock_aws):
    s3_service = S3Service()
    s3_service.copy(test_data.s3_sources, test_data.s3_output_prefix_processed)
    object_list = s3_service.list_objects_contents(test_data.s3_output_prefix_processed)
    assert len(object_list) == 2
    copied_file_content = [
        s3_service.client.get_object(Bucket=S3Service.bucket, Key=copied_obj["Key"])[
            "Body"
        ]
        .read()
        .decode("utf-8")
        for copied_obj in object_list
    ]
    assert copied_file_content == [file_content_1, file_content_2]


def test_copy_raises_incomplete_transaction(mock_aws, mocker):
    mocker.spy(S3Service, "copy")
    s3_service = S3Service()
    stubber = Stubber(s3_service.client)
    stubber.add_client_error("copy_object")
    stubber.activate()
    with pytest.raises(IncompleteTransaction):
        s3_service.copy(test_data.s3_sources, test_data.s3_output_prefix_processed)

    assert S3Service.copy.call_count == 4
    stubber.deactivate()


def test_delete_from_prefix(mock_aws):
    s3_service = S3Service()
    s3_service.delete_from_prefix(test_data.s3_input_prefix)
    assert len(s3_service.list_objects_contents(test_data.s3_input_prefix)) == 0


def test_resolve_s3_sources(mock_aws):
    s3_sources = S3Service().resolve_s3_sources(test_data.s3_input_prefix)
    assert s3_sources == test_data.s3_sources


def test_list_objects(mock_aws):
    s3_objects = S3Service().list_objects_contents(test_data.s3_input_prefix)
    assert len(s3_objects) == len(test_files)


def test_list_more_than_1000_objects():
    s3_client = mock_aws_s3_client(S3Service.bucket)
    for i in range(1100):
        s3_client.put_object(
            Bucket=S3Service.bucket,
            Key=f"{test_data.s3_input_prefix}filename{i}",
            Body="blablabla".encode("utf-8"),
        )

    s3_objects = S3Service().list_objects_contents(test_data.s3_input_prefix)
    assert len(s3_objects) == 1100


@pytest.fixture
def mock_aws():
    s3_client = mock_aws_s3_client(S3Service.bucket)
    for test_file in test_files:
        s3_client.put_object(
            Bucket=S3Service.bucket,
            Key=f"{test_data.s3_input_prefix}{test_file['filename']}",
            Body=test_file["content"],
        )
