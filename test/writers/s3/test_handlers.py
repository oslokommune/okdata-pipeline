import re
from unittest.mock import ANY, call
from dataclasses import asdict

import pytest

import test.writers.s3.test_data as test_data

from okdata.aws.status.sdk import Status


from okdata.pipeline.exceptions import IllegalWrite
from okdata.pipeline.models import StepData
from okdata.pipeline.writers.s3 import handlers
from okdata.pipeline.writers.s3.exceptions import (
    DistributionNotCreated,
    IncompleteTransaction,
)
from okdata.pipeline.writers.s3.services import S3Service
from okdata.sdk.data.dataset import Dataset


def test_copy_to_processed_ok(
    mock_s3_service_ok, mock_dataset_create_distribution_ok, mock_status, mocker
):
    mocker.spy(S3Service, "copy")
    mocker.spy(Dataset, "create_distribution")
    lambda_event = test_data.copy_event("processed")
    response = handlers.write_s3(lambda_event, {})

    assert response == asdict(
        StepData(
            status="OK",
            errors=[],
            s3_input_prefixes={
                test_data.dataset_id: test_data.s3_output_prefix_processed
            },
        )
    )

    S3Service.copy.assert_called_once_with(
        ANY, test_data.s3_sources, test_data.s3_output_prefix_processed
    )
    Dataset.create_distribution.assert_called_once_with(
        ANY,
        test_data.dataset_id,
        test_data.version,
        test_data.edition,
        {
            "distribution_type": "file",
            "content_type": "application/json",
            "filenames": test_data.filenames,
        },
    )


def test_copy_to_processed_ok_without_content_type(
    mock_s3_service_ok, mock_dataset_create_distribution_ok, mock_status, mocker
):
    mocker.spy(Dataset, "create_distribution")
    lambda_event = test_data.copy_event("processed")
    task_config = lambda_event["payload"]["pipeline"]["task_config"]
    task_config["write_to_s3"].pop("content_type")
    handlers.write_s3(lambda_event, {})

    Dataset.create_distribution.assert_called_once_with(
        ANY,
        test_data.dataset_id,
        test_data.version,
        test_data.edition,
        {
            "distribution_type": "file",
            "filenames": test_data.filenames,
        },
    )


def test_copy_to_cleaned_ok(
    mock_s3_service_ok, mock_dataset_create_distribution_ok, mock_status, mocker
):
    mocker.spy(S3Service, "copy")
    mocker.spy(Dataset, "create_distribution")

    lambda_event = test_data.copy_event("cleaned")
    response = handlers.write_s3(lambda_event, {})

    assert response == asdict(
        StepData(
            status="OK",
            errors=[],
            s3_input_prefixes={
                test_data.dataset_id: test_data.s3_output_prefix_cleaned
            },
        )
    )

    S3Service.copy.assert_called_once_with(
        ANY, test_data.s3_sources, test_data.s3_output_prefix_cleaned
    )
    Dataset.create_distribution.call_count == 0


def test_copy_to_processed_latest_ok(
    mock_s3_service_ok,
    mock_dataset_create_distribution_ok,
    mock_status,
    mock_get_latest_edition,
    mocker,
):
    mocker.spy(S3Service, "copy")
    mocker.spy(S3Service, "delete_from_prefix")
    mocker.spy(Dataset, "create_distribution")

    lambda_event = test_data.copy_event("processed", write_to_latest=True)
    response = handlers.write_s3(lambda_event, {})

    assert response == asdict(
        StepData(
            status="OK",
            errors=[],
            s3_input_prefixes={
                test_data.dataset_id: test_data.s3_output_prefix_processed
            },
        )
    )

    S3Service.delete_from_prefix.assert_called_once_with(
        ANY, test_data.s3_output_prefix_processed_latest
    )
    S3Service.copy.assert_has_calls(
        [
            call(ANY, test_data.s3_sources, test_data.s3_output_prefix_processed),
            call(
                ANY, test_data.s3_sources, test_data.s3_output_prefix_processed_latest
            ),
        ]
    )
    assert S3Service.copy.call_count == 2
    Dataset.create_distribution.assert_called_once_with(
        ANY,
        test_data.dataset_id,
        test_data.version,
        test_data.edition,
        {
            "distribution_type": "file",
            "content_type": "application/json",
            "filenames": test_data.filenames,
        },
    )


def test_copy_to_processed_latest_edition_not_latest(
    mock_s3_service_ok,
    mock_dataset_create_distribution_ok,
    mock_status,
    mock_get_latest_edition,
    mocker,
):
    mocker.spy(S3Service, "copy")
    mocker.spy(S3Service, "delete_from_prefix")
    mocker.spy(Dataset, "create_distribution")

    not_latest_edition = "20190120T133701"
    expected_s3_output_prefix = re.sub(
        "edition=.*/",
        f"edition={not_latest_edition}/",
        test_data.s3_output_prefix_processed,
    )
    lambda_event = test_data.copy_event(
        "processed", write_to_latest=True, edition=not_latest_edition
    )
    response = handlers.write_s3(lambda_event, {})

    assert response == asdict(
        StepData(
            status="OK",
            errors=[],
            s3_input_prefixes={test_data.dataset_id: expected_s3_output_prefix},
        )
    )

    assert S3Service.delete_from_prefix.call_count == 0

    S3Service.copy.assert_called_once_with(
        ANY, test_data.s3_sources, expected_s3_output_prefix
    )
    Dataset.create_distribution.assert_called_once_with(
        ANY,
        test_data.dataset_id,
        test_data.version,
        not_latest_edition,
        {
            "distribution_type": "file",
            "content_type": "application/json",
            "filenames": test_data.filenames,
        },
    )


def test_copy_to_processed_incomplete_transaction(
    mock_s3_service_copy_fails, mock_status, mocker
):
    mocker.spy(S3Service, "delete_from_prefix")

    lambda_event = test_data.copy_event("processed")

    with pytest.raises(IncompleteTransaction):
        handlers.write_s3(lambda_event, {})

    S3Service.delete_from_prefix.assert_called_once_with(
        ANY, test_data.s3_output_prefix_processed
    )


def test_copy_to_processed_distribution_not_created(
    mock_s3_service_ok, mock_dataset_create_distribution_fails, mock_status, mocker
):
    mocker.spy(S3Service, "delete_from_prefix")

    lambda_event = test_data.copy_event("processed")

    with pytest.raises(DistributionNotCreated):
        handlers.write_s3(lambda_event, {})

    S3Service.delete_from_prefix.assert_called_once_with(
        ANY, test_data.s3_output_prefix_processed
    )


def test_copy_illegal_input_count(mock_status):
    lambda_event = test_data.copy_event("processed")
    lambda_event["payload"]["step_data"]["s3_input_prefixes"] = {
        "foo": "bar",
        "bar": "foo",
    }

    with pytest.raises(IllegalWrite) as e:
        handlers.write_s3(lambda_event, {})

    assert (
        str(e)
        == "<ExceptionInfo IllegalWrite('illegal write operation: ', 'cannot combine multiple datasets: ', 2) tblen=8>"
    )


@pytest.fixture
def mock_s3_service_ok(monkeypatch):
    def copy(self, s3_sources, output_prefix):
        return

    def resolve_s3_sources(self, source_prefix):
        return test_data.s3_sources

    def delete_from_prefix(self, s3_prefix):
        return

    monkeypatch.setattr(S3Service, "copy", copy)
    monkeypatch.setattr(S3Service, "resolve_s3_sources", resolve_s3_sources)
    monkeypatch.setattr(S3Service, "delete_from_prefix", delete_from_prefix)


@pytest.fixture
def mock_dataset_create_distribution_ok(monkeypatch):
    def create_distribution(self, dataset_id, version, edition, data):
        return 201

    monkeypatch.setattr(Dataset, "create_distribution", create_distribution)


@pytest.fixture
def mock_dataset_create_distribution_fails(monkeypatch):
    def create_distribution(self, dataset_id, version, edition, data):
        raise DistributionNotCreated

    monkeypatch.setattr(Dataset, "create_distribution", create_distribution)


@pytest.fixture
def mock_get_latest_edition(monkeypatch):
    def get_latest_edition(self, dataset_id, version):
        return {"Id": f"{dataset_id}/{version}/{test_data.edition}"}

    monkeypatch.setattr(Dataset, "get_latest_edition", get_latest_edition)


@pytest.fixture
def mock_s3_service_copy_fails(monkeypatch):
    def copy(self, s3_sources, output_prefix):
        raise IncompleteTransaction

    def resolve_s3_sources(self, source_prefix):
        return test_data.s3_sources

    def delete_from_prefix(self, s3_prefix):
        return

    monkeypatch.setattr(S3Service, "copy", copy)
    monkeypatch.setattr(S3Service, "resolve_s3_sources", resolve_s3_sources)
    monkeypatch.setattr(S3Service, "delete_from_prefix", delete_from_prefix)


@pytest.fixture
def mock_status(monkeypatch):
    def _process_payload(self):
        return

    monkeypatch.setattr(Status, "_process_payload", _process_payload)
