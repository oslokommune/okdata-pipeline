import json
import os
from dataclasses import asdict
from pathlib import Path
from unittest.mock import ANY

import pytest

from okdata.aws.status.sdk import Status
from okdata.pipeline.exceptions import IllegalWrite
from okdata.pipeline.models import StepData
from okdata.pipeline.validators.json.handler import format_error_messages, validate_json
from okdata.pipeline.validators.jsonschema_validator import JsonSchemaValidator

test_data_directory = Path(os.path.dirname(__file__), "data")
schema_for_object = json.loads((test_data_directory / "schema.json").read_text())
schema_for_array = json.loads((test_data_directory / "schema_array.json").read_text())
input_events = [{"id": "123", "year": "2021", "datetime": "bar"}]
validation_errors = [
    {"message": "'date' is a required property", "row": "root"},
    {"message": "'bar' is not a 'date-time'", "row": "datetime"},
]
validation_errors_for_array = [
    {"message": "'name' is a required property", "row": 0},
    {"message": "'created' is a required property", "row": 0},
]

task_name = "json_validator"


def test_validation_success(validation_success, mock_status_requests, lambda_event):
    result = validate_json(lambda_event, {})
    JsonSchemaValidator.validate_list.assert_called_once_with(
        self=ANY, data=input_events
    )
    assert result == asdict(
        StepData(
            input_events=input_events,
            status="VALIDATION_SUCCESS",
            errors=[],
        )
    )


def test_validation_failed(
    validation_spy, status_add_spy, mock_status_requests, lambda_event
):
    result = validate_json(lambda_event, {})
    JsonSchemaValidator.validate_list.assert_called_once_with(
        self=ANY, data=input_events
    )
    assert result == asdict(
        StepData(
            input_events=input_events,
            status="VALIDATION_FAILED",
            errors=validation_errors,
        )
    )
    assert status_add_spy.call_count == 2
    assert status_add_spy.call_args == (
        {"errors": format_error_messages(validation_errors)},
    )


def test_validation_failed_for_array(
    validation_spy, status_add_spy, mock_status_requests, lambda_event
):
    array_input = [input_events]
    lambda_event_array_input = lambda_event
    lambda_event_array_input["payload"]["step_data"]["input_events"] = array_input
    lambda_event_array_input["payload"]["pipeline"]["task_config"][task_name][
        "schema"
    ] = schema_for_array

    result = validate_json(lambda_event_array_input, {})
    JsonSchemaValidator.validate_list.assert_called_once_with(
        self=ANY, data=array_input
    )
    assert result == asdict(
        StepData(
            input_events=array_input,
            status="VALIDATION_FAILED",
            errors=validation_errors_for_array,
        )
    )
    assert status_add_spy.call_count == 2
    assert status_add_spy.call_args == (
        {"errors": format_error_messages(validation_errors_for_array)},
    )


def test_no_schema_succeeds(lambda_event, mock_status_requests):
    lambda_event_no_schema = lambda_event
    lambda_event_no_schema["payload"]["pipeline"]["task_config"][task_name] = None
    result = validate_json(lambda_event_no_schema, {})
    assert result == asdict(
        StepData(
            input_events=input_events,
            status="VALIDATION_SUCCESS",
            errors=[],
        )
    )


def test_s3_input(
    lambda_event, spy_read_s3_data, validation_success, mock_status_requests
):
    s3_input = {"foo": "bar"}
    lambda_event_s3 = lambda_event
    lambda_event_s3["payload"]["step_data"]["input_events"] = None
    lambda_event_s3["payload"]["step_data"]["s3_input_prefixes"] = s3_input

    result = validate_json(lambda_event_s3, {})
    assert spy_read_s3_data.call_count == 1
    assert result == asdict(
        StepData(
            input_events=None,
            s3_input_prefixes=s3_input,
            status="VALIDATION_SUCCESS",
            errors=[],
        )
    )


def test_s3_invalid_json(
    lambda_event, status_add_spy, stub_invalid_s3_json_data, mock_status_requests
):
    s3_input = {"foo": "bar"}
    lambda_event_s3 = lambda_event
    lambda_event_s3["payload"]["step_data"]["input_events"] = None
    lambda_event_s3["payload"]["step_data"]["s3_input_prefixes"] = s3_input

    validate_json(lambda_event_s3, {})
    assert status_add_spy.call_count == 2
    assert status_add_spy.call_args == (
        {
            "errors": format_error_messages(
                [
                    {
                        "message": "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"
                    }
                ]
            )
        },
    )


def test_handle_multiple_realtime_events(
    lambda_event, validation_success, mock_status_requests
):
    events = [
        {"foo": "bar"},
        {"bar": "foo"},
    ]
    multiple_realtime_events = lambda_event
    multiple_realtime_events["payload"]["step_data"]["input_events"] = events

    result = validate_json(multiple_realtime_events, {})

    JsonSchemaValidator.validate_list.assert_called_once_with(self=ANY, data=events)
    assert result == asdict(
        StepData(
            input_events=events,
            status="VALIDATION_SUCCESS",
            errors=[],
        )
    )


def test_illegal_input_count_s3(lambda_event, mock_status_requests):
    lambda_event_illegal_input_count_s3 = lambda_event
    lambda_event_illegal_input_count_s3["payload"]["step_data"]["input_events"] = None
    lambda_event_illegal_input_count_s3["payload"]["step_data"]["s3_input_prefixes"] = {
        "foo": "bar",
        "bar": "foo",
    }

    with pytest.raises(IllegalWrite):
        validate_json(lambda_event_illegal_input_count_s3, {})


def test_no_task_config_succeeds(lambda_event, mock_status_requests):
    lambda_event_no_task_config = lambda_event
    lambda_event_no_task_config["payload"]["pipeline"]["task_config"] = None
    result = validate_json(lambda_event_no_task_config, {})
    assert result == asdict(
        StepData(
            input_events=input_events,
            status="VALIDATION_SUCCESS",
            errors=[],
        )
    )


@pytest.fixture
def validation_success(monkeypatch, mocker):
    def validate_list(self, data):
        return []

    monkeypatch.setattr(JsonSchemaValidator, "validate_list", validate_list)
    mocker.spy(JsonSchemaValidator, "validate_list")


@pytest.fixture
def validation_spy(monkeypatch, mocker):
    mocker.spy(JsonSchemaValidator, "validate_list")


@pytest.fixture
def spy_read_s3_data(monkeypatch, mocker):
    import okdata.pipeline.validators.json.handler as json_handler

    def read_s3_data(s3_input_prefix):
        return [""]

    monkeypatch.setattr(json_handler, "read_s3_data", read_s3_data)
    return mocker.spy(json_handler, "read_s3_data")


@pytest.fixture
def stub_invalid_s3_json_data(monkeypatch, mocker):
    import okdata.pipeline.validators.json.handler as json_handler

    def read_s3_data(s3_input_prefix):
        return json.loads("{,}")

    monkeypatch.setattr(json_handler, "read_s3_data", read_s3_data)
    return mocker.spy(json_handler, "read_s3_data")


@pytest.fixture
def lambda_event():
    return {
        "execution_name": "test_execution",
        "task": task_name,
        "payload": {
            "pipeline": {
                "id": "some-id",
                "task_config": {task_name: {"schema": schema_for_object}},
            },
            "output_dataset": {"id": "some-dataset", "version": "some-version"},
            "step_data": {
                "input_events": input_events,
                "status": "PENDING",
                "errors": [],
            },
        },
    }


# Stop HTTP call to status API
@pytest.fixture
def mock_status_requests(monkeypatch):
    def _process_payload(self):
        return

    monkeypatch.setattr(Status, "_process_payload", _process_payload)


@pytest.fixture
def status_add_spy(monkeypatch, mocker):
    import okdata.pipeline.validators.json.handler

    return mocker.spy(okdata.pipeline.validators.json.handler, "status_add")
