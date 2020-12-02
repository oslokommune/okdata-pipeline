from dataclasses import asdict, dataclass

from aws_xray_sdk.core import patch_all, xray_recorder

from okdata.aws.logging import log_add, logging_wrapper
from okdata.pipeline.models import Config, StepData
from okdata.pipeline.validators.jsonschema_validator import JsonSchemaValidator

patch_all()


@dataclass
class StepConfig:
    schema: str = None

    @staticmethod
    def from_dict(step_config_dict):
        if step_config_dict:
            return StepConfig(**step_config_dict)
        else:
            return StepConfig()


@logging_wrapper
@xray_recorder.capture("validate")
def handle(event, context):
    config = Config.from_lambda_event(event)
    step_config = StepConfig.from_dict(config.task_config)
    step_data = config.payload.step_data

    log_add(
        dataset_id=config.payload.output_dataset.id,
        version=config.payload.output_dataset.version,
        edition=config.payload.output_dataset.edition,
    )

    if step_config.schema is None:
        return asdict(
            StepData(
                input_events=step_data.input_events,
                s3_input_prefixes=step_data.s3_input_prefixes,
                status="VALIDATION_SUCCESS",
                errors=[],
            )
        )

    input_data = resolve_input_data(step_data)

    validation_errors = JsonSchemaValidator(step_config.schema).validate_list(
        input_data
    )

    if validation_errors:
        return asdict(
            StepData(
                input_events=step_data.input_events,
                s3_input_prefixes=step_data.s3_input_prefixes,
                status="VALIDATION_FAILED",
                errors=validation_errors[:100],
            )
        )

    return asdict(
        StepData(
            input_events=step_data.input_events,
            s3_input_prefixes=step_data.s3_input_prefixes,
            status="VALIDATION_SUCCESS",
            errors=[],
        )
    )


def resolve_input_data(step_data: StepData):
    if step_data.input_events:
        return step_data.input_events
    elif step_data.s3_input_prefixes:
        return read_s3_data(step_data.s3_input_prefixes)


def read_s3_data(s3_input_prefix):
    raise NotImplementedError
