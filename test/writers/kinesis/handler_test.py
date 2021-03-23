import uuid

import pytest
from aws_xray_sdk.core import xray_recorder
from dateutil.tz import UTC, tzutc
from freezegun.api import freeze_time, FakeDatetime
from moto import mock_kinesis
from requests.exceptions import HTTPError

from okdata.sdk.data.dataset import Dataset
from test.writers.kinesis.util import create_stream, get_records_from_stream

xray_recorder.begin_segment("Test")

utc_now = "2019-06-26T08:55:00+00:00"
uuid_str = "d80ef25f-5f97-4326-b524-50154006f0ba"
dataset_id = "some-id"
version = "some_version"
access_rights = "public"
confidentiality = "green"

input_events = [{"foo": "bar"}, {"foo": "car"}]

lambda_event = lambda_event = {
    "execution_name": "test_execution",
    "task": "kinesis_writer",
    "payload": {
        "pipeline": {"id": "some-id", "task_config": {"kinesis_writer": None}},
        "output_dataset": {"id": dataset_id, "version": version},
        "step_data": {"input_events": input_events, "status": "PENDING", "errors": []},
    },
}


@freeze_time(utc_now)
def test_write_kinesis(mock_uuid, kinesis_writer, mock_dataset_client):
    destination_stream_name = (
        f"dp.{confidentiality}.{dataset_id}.processed.{version}.json"
    )
    create_stream(destination_stream_name)

    kinesis_writer.write_kinesis(lambda_event, {})

    records_on_destination_stream = get_records_from_stream(destination_stream_name)

    for record in records_on_destination_stream:
        record["ApproximateArrivalTimestamp"] = record[
            "ApproximateArrivalTimestamp"
        ].astimezone(UTC)

    expected = [
        {
            "SequenceNumber": "1",
            "ApproximateArrivalTimestamp": FakeDatetime(
                2019, 6, 26, 8, 55, tzinfo=tzutc()
            ),
            "Data": b'{"foo": "bar"}\n',
            "PartitionKey": uuid_str,
        },
        {
            "SequenceNumber": "2",
            "ApproximateArrivalTimestamp": FakeDatetime(
                2019, 6, 26, 8, 55, tzinfo=tzutc()
            ),
            "Data": b'{"foo": "car"}\n',
            "PartitionKey": uuid_str,
        },
    ]

    assert records_on_destination_stream == expected


@pytest.fixture
def mock_uuid(monkeypatch):
    def uuid4():
        return uuid.UUID(uuid_str)

    monkeypatch.setattr(uuid, "uuid4", uuid4)


@pytest.fixture
def kinesis_writer():
    mock_kinesis().start()
    from okdata.pipeline.writers.kinesis import handler

    return handler


@pytest.fixture
def mock_dataset_client(monkeypatch):
    def get_dataset(self, dataset_id, retries=0):
        if dataset_id == "error":
            raise HTTPError
        return {"accessRights": access_rights}

    monkeypatch.setattr(Dataset, "get_dataset", get_dataset)
