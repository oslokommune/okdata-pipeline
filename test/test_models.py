from copy import deepcopy
from dataclasses import FrozenInstanceError

import pytest

from okdata.pipeline.models import Config, OutputDataset, Payload, Pipeline, StepData


event_pipeline_lambda_event = {
    "execution_name": "test_execution",
    "task": "kinesis_writer",
    "payload": {
        "pipeline": {
            "id": "some-id",
            "task_config": {"kinesis_writer": {"some_config": "some_value"}},
        },
        "output_dataset": {"id": "some-id", "version": "1"},
        "step_data": {
            "input_events": [{"foo": "bar"}, {"foo": "car"}],
            "status": "PENDING",
            "errors": [],
        },
    },
}

s3_pipeline_lambda_event = {
    "execution_name": "test_execution",
    "task": "s3_writer",
    "payload": {
        "pipeline": {
            "id": "some-id",
            "task_config": {"s3_writer": {"some_config": "some_value"}},
        },
        "output_dataset": {
            "id": "some-id",
            "version": "1",
            "edition": "some-edition",
            "s3_prefix": "some-s3-prefix",
        },
        "step_data": {
            "s3_input_prefixes": {
                "input1": "some-s3-prefix",
                "input2": "some-s3-prefix",
                "input3": "some-s3-prefix",
            },
            "status": "PENDING",
            "errors": [],
        },
    },
}

event_with_default_task_config = {
    "execution_name": "test_execution",
    "task": "kinesis_writer",
    "payload": {
        "pipeline": {
            "id": "some-id",
            "task_config": {},
        },
        "output_dataset": {"id": "some-id", "version": "1"},
        "step_data": {
            "input_events": [{"foo": "bar"}],
            "status": "PENDING",
            "errors": [],
        },
    },
    "task_config": {"some_config": "some value"},
}

event_overriding_default_task_config = {
    "execution_name": "test_execution",
    "task": "kinesis_writer",
    "payload": {
        "pipeline": {
            "id": "some-id",
            "task_config": {"kinesis_writer": {"some_config": "some other value"}},
        },
        "output_dataset": {"id": "some-id", "version": "1"},
        "step_data": {
            "input_events": [{"foo": "bar"}],
            "status": "PENDING",
            "errors": [],
        },
    },
    "task_config": {"some_config": "some value"},
}

event_with_null_task_config = {
    "execution_name": "test_execution",
    "task": "kinesis_writer",
    "payload": {
        "pipeline": {
            "id": "some-id",
            "task_config": None,
        },
        "output_dataset": {"id": "some-id", "version": "1"},
        "step_data": {
            "input_events": [{"foo": "bar"}],
            "status": "PENDING",
            "errors": [],
        },
    },
    "task_config": {"some_config": "some value"},
}

event_with_missing_task_config = {
    "execution_name": "test_execution",
    "task": "kinesis_writer",
    "payload": {
        "pipeline": {
            "id": "some-id",
        },
        "output_dataset": {"id": "some-id", "version": "1"},
        "step_data": {
            "input_events": [{"foo": "bar"}],
            "status": "PENDING",
            "errors": [],
        },
    },
    "task_config": {"some_config": "some value"},
}

event_with_null_task_config_child = {
    "execution_name": "test_execution",
    "task": "kinesis_writer",
    "payload": {
        "pipeline": {
            "id": "some-id",
            "task_config": {"kinesis_writer": None},
        },
        "output_dataset": {"id": "some-id", "version": "1"},
        "step_data": {
            "input_events": [{"foo": "bar"}, {"foo": "car"}],
            "status": "PENDING",
            "errors": [],
        },
    },
    "task_config": {"some_config": "some value"},
}


def test_config_types():
    config = Config.from_lambda_event(event_pipeline_lambda_event)
    assert isinstance(config.payload, Payload)
    assert isinstance(config.payload.pipeline, Pipeline)
    assert isinstance(config.payload.step_data, StepData)
    assert isinstance(config.payload.output_dataset, OutputDataset)


def test_config_immutable():
    config = Config.from_lambda_event(event_pipeline_lambda_event)
    with pytest.raises(FrozenInstanceError):
        config.execution_name = "bleh"
    with pytest.raises(FrozenInstanceError):
        config.payload.output_dataset.version = "bleh"
    with pytest.raises(FrozenInstanceError):
        config.payload.step_data = StepData("", [], {"foo": "bar"})
    config.payload.step_data.s3_input_prefixes = {"Mutable": "ok"}


def test_config_from_event_pipeline_lambda_event():

    config = Config.from_lambda_event(event_pipeline_lambda_event)

    assert config.execution_name == "test_execution"
    assert config.task == "kinesis_writer"
    assert config.payload.pipeline == Pipeline(
        id="some-id",
        task_config={"kinesis_writer": {"some_config": "some_value"}},
    )
    assert config.payload.output_dataset == OutputDataset(id="some-id", version="1")
    assert config.payload.step_data == StepData(
        input_events=[{"foo": "bar"}, {"foo": "car"}],
        status="PENDING",
        errors=[],
    )
    assert config.payload.step_data.input_count == 2


def test_config_from_s3_pipeline_lambda_event():
    config = Config.from_lambda_event(s3_pipeline_lambda_event)

    assert config.execution_name == "test_execution"
    assert config.task == "s3_writer"
    assert config.payload.pipeline == Pipeline(
        id="some-id",
        task_config={"s3_writer": {"some_config": "some_value"}},
    )
    assert config.payload.output_dataset == OutputDataset(
        id="some-id", version="1", edition="some-edition", s3_prefix="some-s3-prefix"
    )
    assert config.payload.step_data == StepData(
        s3_input_prefixes={
            "input1": "some-s3-prefix",
            "input2": "some-s3-prefix",
            "input3": "some-s3-prefix",
        },
        status="PENDING",
        errors=[],
    )
    assert config.payload.step_data.input_count == 3


def test_config_from_lambda_event_value_error():
    value_error_event_1 = deepcopy(event_pipeline_lambda_event)
    value_error_event_1["payload"]["step_data"] = {
        "input_events": [{"foo": "bar"}, {"foo": "car"}],
        "s3_input_prefixes": {"input1": "some-s3-prefix"},
        "status": "PENDING",
        "errors": [],
    }

    with pytest.raises(ValueError) as e1:
        Config.from_lambda_event(value_error_event_1)

    assert (
        str(e1.value)
        == "Can only set values for one of 's3_input_prefixes' or 'input_events'"
    )

    value_error_event_2 = deepcopy(event_pipeline_lambda_event)
    value_error_event_2["payload"]["step_data"] = {
        "status": "PENDING",
        "errors": [],
    }

    with pytest.raises(ValueError) as e2:
        Config.from_lambda_event(value_error_event_2)

    assert (
        str(e2.value)
        == "Either 's3_input_prefixes' or 'input_events' must be assigned a value"
    )


def test_default_task_config():
    config = Config.from_lambda_event(event_with_default_task_config)

    assert config.task_config["some_config"] == "some value"


def test_override_default_task_config():
    config = Config.from_lambda_event(event_overriding_default_task_config)

    assert config.task_config["some_config"] == "some other value"


def test_handle_null_task_config():
    config = Config.from_lambda_event(event_with_null_task_config)

    assert config.task_config["some_config"] == "some value"


def test_handle_missing_task_config():
    config = Config.from_lambda_event(event_with_missing_task_config)

    assert config.task_config["some_config"] == "some value"


def test_handle_null_task_config_child():
    config = Config.from_lambda_event(event_with_null_task_config_child)

    assert config.task_config["some_config"] == "some value"
