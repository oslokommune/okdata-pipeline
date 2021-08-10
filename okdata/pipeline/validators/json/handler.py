from dataclasses import asdict, dataclass
from json import JSONDecodeError

from aws_xray_sdk.core import patch_all, xray_recorder

from okdata.aws.logging import log_add, logging_wrapper
from okdata.aws.status import status_wrapper, status_add
from okdata.pipeline.exceptions import IllegalWrite
from okdata.pipeline.models import Config, StepData
from okdata.pipeline.validators.json.s3_reader import read_s3_data
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


@status_wrapper
@logging_wrapper
@xray_recorder.capture("validate_json")
def validate_json(event, context):
    config = Config.from_lambda_event(event)
    step_config = StepConfig.from_dict(config.task_config)
    step_data = config.payload.step_data

    output_dataset = config.payload.output_dataset

    log_add(
        dataset_id=output_dataset.id,
        version=output_dataset.version,
        edition=output_dataset.edition,
    )

    status_add(
        domain="dataset",
        domain_id=f"{output_dataset.id}/{output_dataset.version}",
        operation=config.task,
    )

    if step_data.s3_input_prefixes and step_data.input_count > 1:
        raise IllegalWrite(
            "cannot combine multiple S3 datasets: ", step_data.input_count
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

    try:
        input_data = resolve_input_data(step_data)
    except JSONDecodeError as json_error:
        errors = [{"message": str(json_error)}]

        status_add(errors=format_error_messages(errors))

        return asdict(
            StepData(
                input_events=step_data.input_events,
                s3_input_prefixes=step_data.s3_input_prefixes,
                status="VALIDATION_FAILED",
                errors=errors,
            )
        )

    validation_errors = JsonSchemaValidator(step_config.schema).validate_list(
        input_data
    )

    if validation_errors:
        status_add(errors=format_error_messages(validation_errors[:100]))

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
    return None


def format_error_messages(errors):
    # Package `jsonschema` has only English error messages.
    error_details = "\n".join([format_error_message(error) for error in errors])

    return [
        {
            "message": {
                "nb": f"Opplastet JSON er ugyldig.\n\nDetaljer:\n{error_details}",
                "en": f"Uploaded JSON is invalid.\n\nDetails:\n{error_details}",
            }
        }
    ]


def format_error_message(error):
    message = error["message"].rstrip(".")

    if "row" in error:
        row = error["row"]
        row_text = f"index {row}" if isinstance(row, int) else row
        if "col" in error:
            message = "{} at {} on {}".format(message, row_text, error["col"])
        else:
            message = "{} at {}".format(message, row_text)

    return message + "."
