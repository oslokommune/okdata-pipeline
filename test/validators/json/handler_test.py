import json
import os
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from unittest.mock import ANY

import pytest

from okdata.pipeline.exceptions import IllegalWrite
from okdata.pipeline.models import StepData
from okdata.pipeline.validators.json.handler import validate_json
from okdata.pipeline.validators.jsonschema_validator import JsonSchemaValidator

test_data_directory = Path(os.path.dirname(__file__), "data")
input_events = [{"foo", "bar"}]
schema = json.loads((test_data_directory / "schema.json").read_text())
validation_errors = [{"index": 0, "message": "some message"}]
task_name = "json_validator"

lambda_event = {
    "execution_name": "test_execution",
    "task": task_name,
    "payload": {
        "pipeline": {
            "id": "some-id",
            "task_config": {task_name: {"schema": schema}},
        },
        "output_dataset": {"id": "some-dataset", "version": "some-version"},
        "step_data": {"input_events": input_events, "status": "PENDING", "errors": []},
    },
}


def test_validation_success(validation_success):
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


def test_validation_failed(validation_failure):
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


def test_no_schema_succeeds():
    lambda_event_no_schema = deepcopy(lambda_event)
    lambda_event_no_schema["payload"]["pipeline"]["task_config"][task_name] = None
    result = validate_json(lambda_event_no_schema, {})
    assert result == asdict(
        StepData(
            input_events=input_events,
            status="VALIDATION_SUCCESS",
            errors=[],
        )
    )


def test_s3_input(spy_read_s3_data):
    lambda_event_s3 = deepcopy(lambda_event)
    lambda_event_s3["payload"]["step_data"]["input_events"] = None
    lambda_event_s3["payload"]["step_data"]["s3_input_prefixes"] = {"foo": "bar"}

    validate_json(lambda_event_s3, {})
    assert spy_read_s3_data.call_count == 1


def test_illegal_input_count():
    lambda_event_illegal_input_count = deepcopy(lambda_event)
    lambda_event_illegal_input_count["payload"]["step_data"]["input_events"] = [
        {"foo": "bar"},
        {"bar": "foo"},
    ]

    with pytest.raises(IllegalWrite):
        validate_json(lambda_event_illegal_input_count, {})


def test_illegal_input_count_s3():
    lambda_event_illegal_input_count_s3 = deepcopy(lambda_event)
    lambda_event_illegal_input_count_s3["payload"]["step_data"]["input_events"] = None
    lambda_event_illegal_input_count_s3["payload"]["step_data"]["s3_input_prefixes"] = {
        "foo": "bar",
        "bar": "foo",
    }

    with pytest.raises(IllegalWrite):
        validate_json(lambda_event_illegal_input_count_s3, {})


def test_no_task_config_succeeds():
    lambda_event_no_task_config = deepcopy(lambda_event)
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
def validation_failure(monkeypatch, mocker):
    def validate_list(self, data):
        return validation_errors

    monkeypatch.setattr(JsonSchemaValidator, "validate_list", validate_list)
    mocker.spy(JsonSchemaValidator, "validate_list")


@pytest.fixture
def spy_read_s3_data(monkeypatch, mocker):
    import okdata.pipeline.validators.json.handler as json_handler

    def read_s3_data(s3_input_prefix):
        return ""

    monkeypatch.setattr(json_handler, "read_s3_data", read_s3_data)
    return mocker.spy(json_handler, "read_s3_data")
