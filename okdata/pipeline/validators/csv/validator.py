import csv
import json
import os
from dataclasses import dataclass, asdict
from enum import Enum

import boto3
from aws_xray_sdk.core import patch_all, xray_recorder

from okdata.pipeline.models import Config
from okdata.pipeline.validators.csv import string_reader
from okdata.pipeline.validators.csv.jsonschema_validator import JsonSchemaValidator
from okdata.pipeline.validators.csv.parser import ParseErrors, parse_csv
from okdata.aws.logging import log_add, logging_wrapper

patch_all()

s3 = boto3.client("s3")
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
        if type(schema) == str and schema != "":
            self.schema = json.loads(schema)
        else:
            self.schema = schema

    @classmethod
    def from_task_config(cls, event):
        return cls(**event)


@logging_wrapper
@xray_recorder.capture("validate")
def validate(event, context):
    config = Config.from_lambda_event(event)

    step_config = StepConfig.from_task_config(config.task_config)

    s3_prefix = config.payload.output_dataset.s3_prefix

    log_add(
        header_row=step_config.header_row,
        delimiter=step_config.delimiter,
        quote=step_config.quote,
        schema=step_config.schema,
        output_prefix=s3_prefix,
    )

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
        string_reader.from_response(response),
        dialect="unix",
        delimiter=step_config.delimiter,
        quotechar=step_config.quote,
    )
    header = None
    if step_config.header_row:
        header = next(reader)
    try:
        csv_data = parse_csv(reader, step_config.schema, header)
    except ParseErrors as p:
        return _with_error(config, p.errors)

    validation_errors = JsonSchemaValidator(step_config.schema).validate(csv_data)

    if validation_errors:
        return _with_error(config, errors=validation_errors)

    config.payload.step_data.status = Status.VALIDATION_SUCCESS.value
    return asdict(config.payload.step_data)


def _with_error(config: Config, errors):
    log_add(errors=errors)
    log_add(status=Status.VALIDATION_FAILED.value)
    config.payload.step_data.status = Status.VALIDATION_FAILED.value
    config.payload.step_data.errors = errors[:100]
    return asdict(config.payload.step_data)
