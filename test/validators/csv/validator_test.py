import json

import pytest

from okdata.aws.status.sdk import Status
import okdata.pipeline.validators.csv as csv_validator
from okdata.pipeline.validators.csv.validator import (
    StepConfig,
    validate_csv,
    format_errors,
)


def test_config():
    c = StepConfig('{"hello":"ok"}', False, ";", "'")
    assert type(c.schema) == dict


def test_config_from_event(event):
    task_config = event["payload"]["pipeline"]["task_config"]["validate_input"]
    c = StepConfig.from_task_config(task_config)
    assert type(c.schema) == dict
    assert c.schema == json.loads(task_config["schema"])


def csv_generator(*args):
    for element in [
        "int,bool,str,nil",
        "1,false,string,null",
        "2,false,string,null",
        *args,
    ]:
        yield element


def csv_generator_empty():
    yield ""


def test_csv_validator(mocker, event, mock_status):
    s3 = mocker.patch.object(csv_validator.validator, "s3")
    s3.list_objects_v2.return_value = {"Contents": [{"Key": "s3/key"}]}
    string_reader = mocker.patch.object(csv_validator.validator, "string_reader")
    string_reader.from_response.return_value = csv_generator()
    result = validate_csv(event, {})
    assert len(result["errors"]) == 0


def test_csv_validator_empty(mocker, event, mock_status):
    s3 = mocker.patch.object(csv_validator.validator, "s3")
    s3.list_objects_v2.return_value = {"Contents": [{"Key": "s3/key"}]}
    string_reader = mocker.patch.object(csv_validator.validator, "string_reader")
    string_reader.from_response.return_value = csv_generator_empty()
    result = validate_csv(event, {})

    assert len(result["errors"]) == 1


def test_csv_validator_errors(mocker, event, mock_status):
    s3 = mocker.patch.object(csv_validator.validator, "s3")
    s3.list_objects_v2.return_value = {"Contents": [{"Key": "s3/key"}]}
    string_reader = mocker.patch.object(csv_validator.validator, "string_reader")
    string_reader.from_response.return_value = csv_generator(
        "2,2,string,null", "2,true,string,bleh"
    )
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
