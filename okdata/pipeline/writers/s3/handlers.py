import re
from dataclasses import asdict

from aws_xray_sdk.core import patch_all, xray_recorder

from okdata.aws.logging import log_add, log_duration, log_exception, logging_wrapper
from okdata.aws.status import status_add, status_wrapper
from okdata.pipeline.models import Config, StepData
from okdata.pipeline.exceptions import IllegalWrite
from okdata.pipeline.writers.s3.exceptions import (
    DistributionNotCreated,
    IncompleteTransaction,
)
from okdata.pipeline.writers.s3.models import Distribution, TaskConfig
from okdata.pipeline.writers.s3.services import S3Service
from okdata.sdk.data.dataset import Dataset

patch_all()

s3_service = S3Service()


@status_wrapper
@logging_wrapper
@xray_recorder.capture("write_s3")
def write_s3(event, context):
    config = Config.from_lambda_event(event)
    task_config = TaskConfig.from_dict(config.task_config)
    output_dataset = config.payload.output_dataset
    step_data = config.payload.step_data
    content_type = task_config.content_type

    log_add(
        dataset_id=output_dataset.id,
        version=output_dataset.version,
        edition_id=output_dataset.edition,
        source_prefixes=step_data.s3_input_prefixes,
        write_to_latest=task_config.write_to_latest,
        output_stage=task_config.output_stage,
    )
    if content_type:
        log_add(content_type=content_type)

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
            create_distribution_with_retries(output_dataset, copied_files, content_type)
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


def create_distribution_with_retries(
    output_dataset, copied_files, content_type, retries=3
):
    try:
        new_distribution = Distribution(
            filenames=copied_files, content_type=content_type
        )
        return log_duration(
            lambda: Dataset().create_distribution(
                output_dataset.id,
                output_dataset.version,
                output_dataset.edition,
                data=new_distribution.as_dict(),
            ),
            "create_distribution_duration",
        )
    except Exception as e:
        if retries > 0:
            return create_distribution_with_retries(
                output_dataset, copied_files, content_type, retries - 1
            )
        else:
            raise e


def is_latest_edition(dataset_id, version, edition):
    latest_edition = log_duration(
        lambda: Dataset().get_latest_edition(dataset_id, version),
        "get_latest_edition_duration",
    )
    is_latest = [dataset_id, version, edition] == latest_edition["Id"].split("/")
    log_add(is_latest_edition=is_latest)
    return is_latest
