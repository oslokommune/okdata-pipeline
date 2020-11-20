from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class Pipeline:
    id: str
    task_config: dict = None


@dataclass(frozen=True)
class OutputDataset:
    id: str
    version: str
    edition: str = None
    s3_prefix: str = None


@dataclass
class StepData:
    status: str
    errors: list
    s3_input_prefixes: dict = None
    input_events: List[Dict] = None

    def __init__(
        self,
        status,
        errors,
        s3_input_prefixes: dict = None,
        input_events: List[Dict] = None,
    ):
        if input_events and s3_input_prefixes:
            raise ValueError(
                "Can only set values for one of 's3_input_prefixes' or 'input_events'"
            )

        if not (input_events or s3_input_prefixes):
            raise ValueError(
                "Either 's3_input_prefixes' or 'input_events' must be assigned a value"
            )

        self.status = status
        self.errors = errors
        self.s3_input_prefixes = s3_input_prefixes
        self.input_events = input_events

    @property
    def input_count(self):
        if self.s3_input_prefixes:
            return len(self.s3_input_prefixes.items())
        if self.input_events:
            return len(self.input_events)


@dataclass(frozen=True)
class Payload:
    pipeline: Pipeline
    output_dataset: OutputDataset
    step_data: StepData

    @staticmethod
    def from_dict(payload: dict):
        return Payload(
            pipeline=Pipeline(**payload["pipeline"]),
            output_dataset=OutputDataset(**payload["output_dataset"]),
            step_data=StepData(**payload["step_data"]),
        )


@dataclass(frozen=True)
class Config:
    execution_name: str
    task: str
    payload: Payload
    task_config: dict = None

    @staticmethod
    def from_lambda_event(event: dict):
        task = event["task"]
        payload = Payload.from_dict(event["payload"])

        # Get global task config from pipeline template
        task_config = event.get("task_config", {})

        # Update with specific config from pipeline instance
        pipeline_task_configs = payload.pipeline.task_config or {}
        pipeline_task_config = pipeline_task_configs.get(task, {}) or {}
        task_config.update(pipeline_task_config)

        return Config(
            execution_name=event["execution_name"],
            task=task,
            payload=payload,
            task_config=task_config,
        )
