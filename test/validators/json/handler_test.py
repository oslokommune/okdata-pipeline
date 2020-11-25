import json
from copy import deepcopy
from dataclasses import asdict
from unittest.mock import ANY

import pytest

from okdata.pipeline.models import StepData
from okdata.pipeline.validators.json.handler import handle
from okdata.pipeline.validators.json.jsonschema_validator import JsonSchemaValidator
from test.validators.json.conftest import test_data_directory

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
    result = handle(lambda_event, {})
    JsonSchemaValidator.validate.assert_called_once_with(
        self=ANY, json_data=input_events
    )
    assert result == asdict(
        StepData(
            input_events=input_events,
            status="VALIDATION_SUCCESS",
            errors=[],
        )
    )


def test_validation_failed(validation_failure):
    result = handle(lambda_event, {})
    JsonSchemaValidator.validate.assert_called_once_with(
        self=ANY, json_data=input_events
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
    result = handle(lambda_event_no_schema, {})
    assert result == asdict(
        StepData(
            input_events=input_events,
            status="VALIDATION_SUCCESS",
            errors=[],
        )
    )


def test_s3_input():
    lambda_event_s3 = deepcopy(lambda_event)
    lambda_event_s3["payload"]["step_data"]["input_events"] = None
    lambda_event_s3["payload"]["step_data"]["s3_input_prefixes"] = {"foo": "bar"}

    with pytest.raises(NotImplementedError):
        handle(lambda_event_s3, {})


def test_no_task_config_succeeds():
    lambda_event_no_task_config = deepcopy(lambda_event)
    lambda_event_no_task_config["payload"]["pipeline"]["task_config"] = None
    result = handle(lambda_event_no_task_config, {})
    assert result == asdict(
        StepData(
            input_events=input_events,
            status="VALIDATION_SUCCESS",
            errors=[],
        )
    )


@pytest.fixture
def validation_success(monkeypatch, mocker):
    def validate(self, json_data):
        return []

    monkeypatch.setattr(JsonSchemaValidator, "validate", validate)
    mocker.spy(JsonSchemaValidator, "validate")


@pytest.fixture
def validation_failure(monkeypatch, mocker):
    def validate(self, json_data):
        return validation_errors

    monkeypatch.setattr(JsonSchemaValidator, "validate", validate)
    mocker.spy(JsonSchemaValidator, "validate")
