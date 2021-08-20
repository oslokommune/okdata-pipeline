import json
import os

import pytest
from moto import mock_s3
from okdata.aws.status.sdk import Status

from okdata.pipeline.validators.csv.validator import (
    StepConfig,
    format_errors,
    validate_csv,
)
from test.util import mock_aws_s3_client

bucket = os.environ["BUCKET_NAME"]


def test_config():
    c = StepConfig('{"hello":"ok"}', False, ";", "'")
    assert type(c.schema) == dict


def test_config_from_event(event):
    task_config = event["payload"]["pipeline"]["task_config"]["validate_input"]
    c = StepConfig.from_task_config(task_config)
    assert type(c.schema) == dict
    assert c.schema == json.loads(task_config["schema"])


def _put_s3(event, input_file, gzipped=False):
    prefix = event["payload"]["step_data"]["s3_input_prefixes"]["boligpriser"]
    s3_mock = mock_aws_s3_client(bucket)
    key = "{}t.csv{}".format(prefix, ".gz" if gzipped else "")

    with open(f"test/validators/csv/data/{input_file}", "rb" if gzipped else "r") as f:
        s3_mock.put_object(Bucket=bucket, Key=key, Body=f.read())


@mock_s3
def test_csv_validator(event):
    _put_s3(event, "valid.csv")
    result = validate_csv(event, {})
    assert len(result["errors"]) == 0


@mock_s3
def test_csv_validator_gzip(event):
    _put_s3(event, "valid.csv.gz", gzipped=True)
    result = validate_csv(event, {})
    assert len(result["errors"]) == 0


@mock_s3
def test_csv_validator_empty(event):
    _put_s3(event, "empty.csv")
    result = validate_csv(event, {})
    assert len(result["errors"]) == 1


@mock_s3
def test_csv_validator_errors(event, mock_status):
    _put_s3(event, "invalid.csv")
    try:
        validate_csv(event, {})
    except Exception as e:
        error_list = e.args[0]
        assert len(error_list) == 2


def test_format_errors():
    e = {
        "row": 1,
        "column": "Bydelsnr",
        "message": "could not convert string float: 'gamle oslo'",
    }
    result = format_errors(e, "nb")
    assert (
        result
        == "Feil på linje 1, kolonne Bydelsnr. Mer beskrivelse: could not convert string float: 'gamle oslo'"
    )


@pytest.fixture
def mock_status(monkeypatch):
    def _process_payload(self):
        return

    monkeypatch.setattr(Status, "_process_payload", _process_payload)
