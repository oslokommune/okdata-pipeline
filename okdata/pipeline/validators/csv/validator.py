import csv
import json
import os
from dataclasses import dataclass, asdict
from enum import Enum

import boto3
from aws_xray_sdk.core import patch_all, xray_recorder
from okdata.aws.logging import log_add, logging_wrapper
from okdata.aws.status import status_wrapper, status_add

from okdata.pipeline.models import Config
from okdata.pipeline.util import sdk_config
from okdata.pipeline.validators.csv import string_reader
from okdata.pipeline.validators.csv.parser import ParseErrors, parse_csv
from okdata.pipeline.validators.jsonschema_validator import JsonSchemaValidator

patch_all()

BUCKET = os.environ["BUCKET_NAME"]


class Status(Enum):
    VALIDATION_SUCCESS = "VALIDATION_SUCCESS"
    VALIDATION_FAILED = "VALIDATION_FAILED"


@dataclass
class StepConfig:
    def __init__(self, schema="", header_row=True, delimiter=",", quote='"'):
        if len(delimiter) != 1:
            raise ValueError("delimiter must be a 1-character string: ", delimiter)

        self.header_row = header_row
        self.delimiter = delimiter
        self.quote = quote
        if isinstance(schema, str) and schema != "":
            self.schema = json.loads(schema)
        else:
            self.schema = schema

    @classmethod
    def from_task_config(cls, event):
        return cls(**event)


@status_wrapper(sdk_config())
@logging_wrapper
@xray_recorder.capture("validate_csv")
def validate_csv(event, context):
    s3 = boto3.client("s3")
    config = Config.from_lambda_event(event)
    output_dataset = config.payload.output_dataset
    step_config = StepConfig.from_task_config(config.task_config)

    s3_prefix = config.payload.output_dataset.s3_prefix

    status_add(
        domain="dataset",
        domain_id=f"{output_dataset.id}/{output_dataset.version}",
        operation=config.task,
    )

    log_add(
        header_row=step_config.header_row,
        delimiter=step_config.delimiter,
        quote=step_config.quote,
        schema=step_config.schema,
        output_prefix=s3_prefix,
    )

    # FIXME: The validator is running too slowly on Deichman's three largest
    # datasets (7,8 million lines and up). Skip validation for schema-less
    # pipelines still, until the validator can handle them.
    if not step_config.schema:
        log_add(notice="No Schema provided for validation")
        config.payload.step_data.status = Status.VALIDATION_SUCCESS.value
        # 2020.06: Validation done optionally - we now return ok if we don't supply a
        # schema for the validation step
        return asdict(config.payload.step_data)

    input_prefix = next(iter(config.payload.step_data.s3_input_prefixes.values()))
    log_add(s3_input_prefix=input_prefix)
    objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=input_prefix)

    s3_path = next(iter(objects["Contents"]))["Key"]
    log_add(s3_input_path=s3_path)

    response = s3.get_object(Bucket=BUCKET, Key=s3_path)
    reader = csv.reader(
        string_reader.from_response(response, gzipped=s3_path.endswith(".gz")),
        dialect="unix",
        delimiter=step_config.delimiter,
        quotechar=step_config.quote,
    )
    header = None
    if step_config.header_row:
        try:
            header = next(reader)
        except StopIteration:
            status_add(
                errors=[
                    {
                        "message": {
                            "nb": "Denne filen mangler header.",
                            "en": "This file has no header.",
                        }
                    }
                ]
            )
            return _with_error(
                config,
                [
                    {
                        "message": {
                            "nb": "Filen mangler header",
                            "en": "This file has no header.",
                        }
                    }
                ],
            )

    try:
        csv_data = parse_csv(reader, step_config.schema, header)
        if not csv_data:
            status_add(
                errors=[
                    {
                        "message": {
                            "nb": "Dette var en tom fil. Fyll den med data.",
                            "en": "This was an empty file. Fill the file with data.",
                        }
                    }
                ]
            )
            return _with_error(
                config, [{"message": {"nb": "Tom fil", "en": "Empty file."}}]
            )

    except ParseErrors as p:
        status_add(
            errors=[
                {
                    "message": {
                        "nb": "\n".join([format_errors(e, "nb") for e in p.errors]),
                        "en": "\n".join([format_errors(e, "en") for e in p.errors]),
                    }
                }
            ]
        )
        return _with_error(config, p.errors)

    if not step_config.schema:
        log_add(notice="No Schema provided for validation")
        config.payload.step_data.status = Status.VALIDATION_SUCCESS.value
        # 2020.06: Validation done optionally - we now return ok if we don't supply a
        # schema for the validation step
        return asdict(config.payload.step_data)

    # Cut off after the first 20 error messages, otherwise the payload may get
    # too big for the status API.
    validation_errors = JsonSchemaValidator(step_config.schema).validate(csv_data)[:20]

    if validation_errors:
        status_add(
            errors=[
                {
                    "message": {
                        "nb": "\n".join(
                            [format_errors(e, "nb") for e in validation_errors]
                        ),
                        "en": "\n".join(
                            [format_errors(e, "en") for e in validation_errors]
                        ),
                    }
                }
            ]
        )
        return _with_error(config, errors=validation_errors)

    config.payload.step_data.status = Status.VALIDATION_SUCCESS.value
    return asdict(config.payload.step_data)


def _with_error(config: Config, errors):
    log_add(errors=errors)
    log_add(status=Status.VALIDATION_FAILED.value)
    config.payload.step_data.status = Status.VALIDATION_FAILED.value
    config.payload.step_data.errors = errors[:100]
    return asdict(config.payload.step_data)


def format_errors(errors, language):
    line = errors["row"]
    column = errors.get("column")
    message = errors["message"]

    if language == "nb":
        return "Feil på linje {}{}: {}".format(
            line,
            f", kolonne {column}" if column else "",
            message,
        )
    else:
        return "Error on line {}{}: {}".format(
            line,
            f", column {column}" if column else "",
            message,
        )
