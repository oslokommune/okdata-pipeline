import os
import re
from dataclasses import asdict

from aws_xray_sdk.core import patch_all, xray_recorder

from okdata.aws.logging import log_add, log_exception, logging_wrapper
from okdata.aws.status import status_add, status_wrapper
from okdata.pipeline.models import Config, StepData
from okdata.pipeline.writers.s3.exceptions import (
    DistributionNotCreated,
    IllegalWrite,
    IncompleteTransaction,
)
from okdata.pipeline.writers.s3.models import Distribution, TaskConfig
from okdata.pipeline.writers.s3.services import S3Service
from okdata.sdk.config import Config as OrigoSdkConfig
from okdata.sdk.data.dataset import Dataset

patch_all()

s3_service = S3Service()

origo_config = OrigoSdkConfig()
origo_config.config["cacheCredentials"] = False
origo_config.config["client_id"] = os.environ["CLIENT_ID"]
origo_config.config["client_secret"] = os.environ["CLIENT_SECRET"]
dataset_client = Dataset(origo_config)


@status_wrapper
@logging_wrapper
@xray_recorder.capture("copy")
def copy(event, context):
    config = Config.from_lambda_event(event)
    task_config = TaskConfig.from_dict(config.task_config)
    output_dataset = config.payload.output_dataset
    step_data = config.payload.step_data

    log_add(
        dataset_id=output_dataset.id,
        version=output_dataset.version,
        edition_id=output_dataset.edition,
        source_prefixes=step_data.s3_input_prefixes,
        write_to_latest=task_config.write_to_latest,
        output_stage=task_config.output_stage,
    )
    status_add(
        domain="dataset",
        domain_id=f"{output_dataset.id}/{output_dataset.version}",
        operation=config.task,
    )

    if step_data.input_count > 1:
        raise IllegalWrite("cannot combine multiple datasets: ", step_data.input_count)

    source_prefix = next(iter(step_data.s3_input_prefixes.values()))
    output_prefix = config.payload.output_dataset.s3_prefix.replace(
        "%stage%", task_config.output_stage
    )

    s3_sources = s3_service.resolve_s3_sources(source_prefix)
    copied_files = copy_data(s3_sources, output_prefix)

    if task_config.output_stage == "processed":
        try:
            create_distribution_with_retries(output_dataset, copied_files)
        except Exception as e:
            s3_service.delete_from_prefix(output_prefix)
            log_exception(e)
            raise DistributionNotCreated

    if task_config.write_to_latest and is_latest_edition(
        output_dataset.id, output_dataset.version, output_dataset.edition
    ):
        write_data_to_latest(s3_sources, output_prefix)

    output_prefixes = {output_dataset.id: output_prefix}
    response = StepData(s3_input_prefixes=output_prefixes, status="OK", errors=[])

    # TODO: this is just to verify that we have a correct implementation of the status API
    # temporary - if we are in /latest write -> set run to complete
    # Once we get this up and see what the status-api can return to the CLI we will update with more information
    status_body = {
        "files": [s3_source.key for s3_source in s3_sources],
        "latest": task_config.write_to_latest,
    }
    status_add(status_body=status_body)
    return asdict(response)


def write_data_to_latest(s3_sources, output_prefix):
    output_prefix_latest = re.sub("edition=.*/", "latest/", output_prefix)
    s3_service.delete_from_prefix(output_prefix_latest)
    copy_data(s3_sources, output_prefix_latest)


def copy_data(s3_sources, output_prefix):
    try:
        s3_service.copy(s3_sources, output_prefix)
    except IncompleteTransaction as e:
        s3_service.delete_from_prefix(output_prefix)
        raise e

    return [s3_source.filename for s3_source in s3_sources]


def create_distribution_with_retries(output_dataset, copied_files, retries=3):
    try:
        new_distribution = Distribution(filenames=copied_files)
        dataset_client.create_distribution(
            output_dataset.id,
            output_dataset.version,
            output_dataset.edition,
            data=new_distribution.__dict__,
        )
    except Exception as e:
        if retries > 0:
            return create_distribution_with_retries(
                output_dataset, copied_files, retries - 1
            )
        else:
            raise e


@logging_wrapper
@xray_recorder.capture("is_latest_edition")
def is_latest_edition_handler(event, context):
    config = Config.from_lambda_event(event)
    output = config.payload.step_data
    output_dataset = config.payload.output_dataset
    output.status = is_latest_edition(
        output_dataset.id, output_dataset.version, output_dataset.edition
    )
    return asdict(output)


def is_latest_edition(dataset_id, version, edition):
    latest_edition_response = dataset_client.get_latest_edition(dataset_id, version)
    dataset_id, version_id, edition_id = latest_edition_response["Id"].split("/")
    _is_latest_edition = (
        dataset_id == dataset_id and version_id == version and edition_id == edition
    )
    log_add(is_latest_edition=_is_latest_edition)
    return _is_latest_edition