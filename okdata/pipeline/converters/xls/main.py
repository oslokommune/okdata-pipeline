import os
import tempfile
from dataclasses import asdict

import boto3

from okdata.pipeline.converters.xls.TableConfig import TableConfig
from okdata.pipeline.converters.xls.TableConverter import TableConverter
from okdata.pipeline.models import Config

CWD = os.path.dirname(os.path.realpath(__file__))
BUCKET = os.environ["BUCKET_NAME"]

s3 = boto3.resource("s3", region_name="eu-west-1")
s3_client = boto3.client("s3")


def handler(event, context):
    config = Config.from_lambda_event(event)
    task = config.task
    pipeline = config.payload.pipeline
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
        output_dataset.s3_prefix.replace("%stage%", "intermediate") + task + "/"
    )
    table_config = TableConfig(pipeline.task_config.get(task))

    response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=input_prefix)

    for content in response["Contents"]:
        xlsInput = content["Key"]

        filename = xlsInput[len(input_prefix) :]
        filename_prefix = filename[0 : filename.lower().rfind(".xls")]

        output = output_prefix + filename_prefix + ".csv"

        convert_to_csv(xlsInput, output, table_config)

    config.payload.step_data.s3_input_prefixes = {output_dataset.id: output_prefix}
    config.payload.step_data.status = "OK"
    return asdict(config.payload.step_data)


def convert_to_csv(xlsInput, output, config):
    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmpfile:
        s3.Bucket(BUCKET).download_file(xlsInput, tmpfile.name)

        conv = TableConverter(config)
        wb = conv.read_excel_table(tmpfile.name)
        df = conv.convert_table(wb)
        csv = df.to_csv(sep=";", index=False)

        s3.Object(BUCKET, output).put(
            Body=csv, ContentType="text/csv", ContentEncoding="utf-8"
        )
