import re

import pytest
from requests.exceptions import HTTPError

from okdata.pipeline.writers.kinesis.dataset_client import DatasetClient

dataset_client = DatasetClient()

dataset_id = "some-id"
confidentiality = "green"


def test_get_dataset(mock_http_requests):
    assert dataset_client.get_dataset(dataset_id) == {
        "confidentiality": confidentiality
    }


def test_retries(mock_http_error, mocker):
    mocker.spy(DatasetClient, "get_dataset")
    with pytest.raises(HTTPError):
        dataset_client.get_dataset(dataset_id, retries=3)

    assert DatasetClient.get_dataset.call_count == 4


@pytest.fixture
def mock_http_requests(requests_mock):

    requests_mock.register_uri(
        "GET",
        re.compile(f"{dataset_client.metadata_api_url}/datasets/{dataset_id}"),
        json={"confidentiality": confidentiality},
        status_code=200,
    )


@pytest.fixture
def mock_http_error(requests_mock):

    requests_mock.register_uri(
        "GET",
        re.compile(f"{dataset_client.metadata_api_url}/datasets/{dataset_id}"),
        json={"message": "Error"},
        status_code=502,
    )
