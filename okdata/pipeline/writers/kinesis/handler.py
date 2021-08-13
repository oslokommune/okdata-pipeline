import json
import uuid
from dataclasses import asdict

import boto3
from aws_xray_sdk.core import patch_all, xray_recorder

from okdata.aws.logging import logging_wrapper, log_add
from okdata.pipeline.common import CONFIDENTIALITY_MAP
from okdata.pipeline.models import Config, StepData
from okdata.sdk.data.dataset import Dataset

kinesis_client = boto3.client("kinesis", region_name="eu-west-1")

patch_all()


@logging_wrapper
@xray_recorder.capture("write_kinesis")
def write_kinesis(event, context):
    pipeline_config = Config.from_lambda_event(event)

    dataset_id = pipeline_config.payload.output_dataset.id
    version = pipeline_config.payload.output_dataset.version
    log_add(dataset_id=dataset_id, version=version)

    dataset = Dataset().get_dataset(dataset_id, retries=3)
    access_rights = dataset["accessRights"]
    confidentiality = CONFIDENTIALITY_MAP[access_rights]

    output_stream_name = f"dp.{confidentiality}.{dataset_id}.processed.{version}.json"
    log_add(output_stream_name=output_stream_name)

    input_events = pipeline_config.payload.step_data.input_events
    write_to_kinesis(events=input_events, stream_name=output_stream_name)

    return asdict(StepData(input_events=input_events, status="OK", errors=[]))


def write_to_kinesis(events, stream_name):
    records = [
        {"Data": json.dumps(event) + "\n", "PartitionKey": str(uuid.uuid4())}
        for event in events
    ]
    log_add(num_records=len(records))
    kinesis_client.put_records(StreamName=stream_name, Records=records)
