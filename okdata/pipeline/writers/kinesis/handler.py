import json
import os
import uuid
from dataclasses import asdict

import boto3
from aws_xray_sdk.core import patch_all, xray_recorder
from requests.exceptions import HTTPError, Timeout

from okdata.aws.logging import logging_wrapper, log_add
from okdata.pipeline.common import CONFIDENTIALITY_MAP
from okdata.pipeline.models import Config, StepData
from okdata.sdk.config import Config as OrigoSdkConfig
from okdata.sdk.data.dataset import Dataset

kinesis_client = boto3.client("kinesis", region_name="eu-west-1")
s3_client = boto3.client("s3", region_name="eu-west-1")

origo_config = OrigoSdkConfig()
origo_config.config["cacheCredentials"] = False
origo_config.config["client_id"] = os.environ["CLIENT_ID"]
origo_config.config["client_secret"] = os.environ["CLIENT_SECRET"]
dataset_client = Dataset(origo_config)

patch_all()


def get_dataset(dataset_id, retries=2):
    """Return the dataset belonging to `dataset_id`.

    Retry calling the API `retries` number of times in the event of HTTP errors
    or timeouts.

    TODO: Use the SDK directly when/if it grows builtin support for retries.
    """
    try:
        return dataset_client.get_dataset(dataset_id)
    except (HTTPError, Timeout) as e:
        if retries > 0:
            return get_dataset(dataset_id, retries - 1)
        raise e


@logging_wrapper
@xray_recorder.capture("write_kinesis")
def write_kinesis(event, context):
    pipeline_config = Config.from_lambda_event(event)

    dataset_id = pipeline_config.payload.output_dataset.id
    version = pipeline_config.payload.output_dataset.version
    log_add(dataset_id=dataset_id, version=version)

    dataset = get_dataset(dataset_id)
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
