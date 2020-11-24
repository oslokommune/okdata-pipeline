import os
import pathlib
import shutil

import boto3
import pytest
from moto import mock_s3

from okdata.pipeline.converters.csv.base import BUCKET

pwd = pathlib.Path(__file__).parent.absolute()

input_path = f"{pwd}/data/husholdninger.csv"
input_path_gzip = f"{pwd}/data/husholdninger.csv.gz"
output_path = "test/converters/csv/data/husholdninger.parquet"
input_path_tsv = f"{pwd}/data/husholdninger.tsv"
input_path_tsv_gzip = f"{pwd}/data/husholdninger.tsv.gz"
input_path_correct_schema = f"{pwd}/data/schema_correct.csv"
input_path_wrong_schema = f"{pwd}/data/schema_wrong.csv"
input_path_wrong_schema_ng = f"{pwd}/data/schema_wrong_ng.csv"
input_path_schema_ng = f"{pwd}/data/schema_ng.csv"
input_path_dates = f"{pwd}/data/schema_dates.csv"
input_path_dates_year_too_early = f"{pwd}/data/schema_dates_year_too_early.csv"
input_path_dates_year_too_late = f"{pwd}/data/schema_dates_year_too_late.csv"
input_path_dates_date_string_value = f"{pwd}/data/schema_dates_date_string_value.csv"
input_path_dates_date_time_wrong = f"{pwd}/data/schema_dates_date_time_wrong.csv"
input_path_dates_date_time = f"{pwd}/data/schema_dates_date_time.csv"


@pytest.fixture
def event_ng():
    def event_func(input_prefix, chunksize=100, schema=None):
        event = {
            "execution_name": "boligpriser-UUID",
            "task": "csv_exporter",
            "payload": {
                "pipeline": {
                    "id": "husholdninger-med-barn",
                    "task_config": {
                        "csv_exporter": {
                            "format": "parquet",
                            "delimiter": ",",
                            "chunksize": chunksize,
                        }
                    },
                },
                "output_dataset": {
                    "id": "boligpriser",
                    "version": "1",
                    "edition": "20200120T133701",
                    "s3_prefix": "%stage%/green/boligpriser/version=1/edition=20200120T133701/",
                },
                "step_data": {
                    "s3_input_prefixes": {"boligpriser": input_prefix},
                    "status": "OK",
                    "errors": [],
                },
            },
        }
        if schema:
            event["payload"]["pipeline"]["task_config"]["csv_exporter"][
                "schema"
            ] = schema
        return event

    return event_func


@pytest.fixture()
def s3_mock():
    with mock_s3():
        import boto3

        yield boto3.client("s3")


@pytest.fixture
def schema_wrong_ng():
    return lambda: single_input(
        boto3.client("s3"), input_path_wrong_schema_ng, "s3/prefix/"
    )


@pytest.fixture
def schema_ng():
    return lambda: single_input(boto3.client("s3"), input_path_schema_ng, "s3/prefix/")


@pytest.fixture
def husholdninger_single():
    return lambda: single_input(boto3.client("s3"), input_path, "s3/prefix/")


@pytest.fixture
def husholdninger_multiple():
    return lambda: multiple_inputs(
        boto3.client("s3"), f"{pwd}/data/multiple/", "s3/prefix/"
    )


@pytest.fixture
def dates_file():
    return lambda: single_input(boto3.client("s3"), input_path_dates, "s3/prefix/")


@pytest.fixture
def dates_file_year_too_early():
    return lambda: single_input(
        boto3.client("s3"), input_path_dates_year_too_early, "s3/prefix/"
    )


@pytest.fixture
def dates_file_year_too_late():
    return lambda: single_input(
        boto3.client("s3"), input_path_dates_year_too_late, "s3/prefix/"
    )


@pytest.fixture
def dates_file_date_string_value():
    return lambda: single_input(
        boto3.client("s3"), input_path_dates_date_string_value, "s3/prefix/"
    )


@pytest.fixture
def dates_file_date_time_wrong():
    return lambda: single_input(
        boto3.client("s3"), input_path_dates_date_time_wrong, "s3/prefix/"
    )


@pytest.fixture
def dates_file_date_time():
    return lambda: single_input(
        boto3.client("s3"), input_path_dates_date_time, "s3/prefix/"
    )


def single_input(client, test_file, input_prefix):
    client.create_bucket(
        Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
    )
    with open(test_file) as f:
        client.put_object(
            Body=f.read(), Bucket=BUCKET, Key=input_prefix + os.path.basename(f.name)
        )
    return input_prefix, test_file


def multiple_inputs(client, test_file_folder, input_prefix):
    client.create_bucket(
        Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
    )
    files = os.listdir(test_file_folder)
    for file in files:
        with open(f"{test_file_folder}/{file}") as f:
            client.put_object(
                Body=f.read(),
                Bucket=BUCKET,
                Key=input_prefix + os.path.basename(f.name),
            )
    return input_prefix, [f"{test_file_folder}/{file}" for file in files]


@pytest.fixture
def cleanup():
    # before each
    yield
    # after each
    if os.path.exists(output_path):
        if os.path.isdir(output_path) and not os.path.islink(output_path):
            shutil.rmtree(output_path)
        else:
            os.remove(output_path)
