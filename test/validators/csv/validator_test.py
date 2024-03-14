import json

import pytest

from okdata.aws.status.sdk import Status

from okdata.pipeline.validators.csv.validator import (
    StepConfig,
    format_errors,
    validate_csv,
)


def test_config():
    c = StepConfig('{"hello":"ok"}', False, ";", "'")
    assert isinstance(c.schema, dict)


def test_config_from_event(event):
    task_config = event["payload"]["pipeline"]["task_config"]["validate_input"]
    c = StepConfig.from_task_config(task_config)
    assert isinstance(c.schema, dict)
    assert c.schema == json.loads(task_config["schema"])


def _put_s3(s3_client, bucket, event, input_file, gzipped=False):
    prefix = event["payload"]["step_data"]["s3_input_prefixes"]["boligpriser"]
    key = "{}t.csv{}".format(prefix, ".gz" if gzipped else "")

    with open(f"test/validators/csv/data/{input_file}", "rb" if gzipped else "r") as f:
        s3_client.put_object(Bucket=bucket, Key=key, Body=f.read())


def test_csv_validator(s3_client, s3_bucket, event):
    _put_s3(s3_client, s3_bucket, event, "valid.csv")
    result = validate_csv(event, {})
    assert len(result["errors"]) == 0


@pytest.mark.parametrize(
    "input_file",
    [
        "valid.csv.gz",
        "valid-no-final-newline.csv.gz",
        "valid-multibyte.csv.gz",
    ],
)
def test_csv_validator_gzip(s3_client, s3_bucket, event, input_file):
    _put_s3(s3_client, s3_bucket, event, input_file, gzipped=True)
    result = validate_csv(event, {})
    assert len(result["errors"]) == 0


def test_csv_validator_empty(s3_client, s3_bucket, event):
    _put_s3(s3_client, s3_bucket, event, "empty.csv")
    result = validate_csv(event, {})
    assert len(result["errors"]) == 1


def test_csv_validator_errors(s3_client, s3_bucket, event, mock_status):
    _put_s3(s3_client, s3_bucket, event, "invalid.csv")
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
