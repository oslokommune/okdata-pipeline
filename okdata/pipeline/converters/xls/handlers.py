from dataclasses import asdict

import boto3

from okdata.pipeline.converters.base import BUCKET
from okdata.pipeline.converters.xls.TableConfig import TableConfig
from okdata.pipeline.models import Config
from okdata.pipeline.converters.xls.export import convert_to_csv, DeltaExporter


def xlsx_to_csv(event, context):
    s3_client = boto3.client("s3")
    config = Config.from_lambda_event(event)
    output_dataset = config.payload.output_dataset
    step_data = config.payload.step_data

    input_prefixes = step_data.s3_input_prefixes
    if step_data.input_count < 1:
        raise ValueError("No input dataset prefix defined")
    if step_data.input_count > 1:
        raise ValueError(f"Too many dataset inputs: {input_prefixes}")

    input_dataset = list(input_prefixes)[0]
    input_prefix = input_prefixes[input_dataset]
    output_prefix = (
        output_dataset.s3_prefix.replace("%stage%", "intermediate") + config.task + "/"
    )
    table_config = TableConfig(config.task_config)

    response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=input_prefix)

    for content in response["Contents"]:
        xlsx_input = content["Key"]

        filename = xlsx_input[len(input_prefix) :]
        filename_prefix = filename[0 : filename.lower().rfind(".xls")]

        convert_to_csv(
            xlsx_input, f"{output_prefix}{filename_prefix}.csv", table_config
        )

    config.payload.step_data.s3_input_prefixes = {output_dataset.id: output_prefix}
    config.payload.step_data.status = "OK"
    return asdict(config.payload.step_data)


def xlsx_to_delta(event, context):
    return DeltaExporter(event).export()
