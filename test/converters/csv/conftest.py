import os
import pathlib
import shutil

import pytest

from okdata.pipeline.converters.base import BUCKET

pwd = pathlib.Path(__file__).parent.absolute()

input_path = f"{pwd}/data/husholdninger.csv"
input_path_gzip = f"{pwd}/data/husholdninger.csv.gz"
output_path = "test/converters/csv/data/husholdninger.parquet"
input_path_tsv = f"{pwd}/data/husholdninger.tsv"
input_path_tsv_gzip = f"{pwd}/data/husholdninger.tsv.gz"
input_path_correct_schema = f"{pwd}/data/schema_correct.csv"
input_path_wrong_schema = f"{pwd}/data/schema_wrong.csv"
input_path_schema = f"{pwd}/data/schema.csv"
input_path_dates = f"{pwd}/data/schema_dates.csv"
input_path_dates_year_too_early = f"{pwd}/data/schema_dates_year_too_early.csv"
input_path_dates_year_too_late = f"{pwd}/data/schema_dates_year_too_late.csv"
input_path_dates_date_string_value = f"{pwd}/data/schema_dates_date_string_value.csv"
input_path_dates_date_with_time = f"{pwd}/data/schema_dates_date_with_time.csv"
input_path_dates_date_wrong = f"{pwd}/data/schema_dates_date_wrong.csv"
input_path_datetimes = f"{pwd}/data/schema_datetimes.csv"
input_path_datetimes_with_tz = f"{pwd}/data/schema_datetimes_with_tz.csv"
input_path_datetimes_without_time = f"{pwd}/data/schema_datetimes_without_time.csv"
input_path_datetimes_mixed_formats = f"{pwd}/data/schema_datetimes_mixed_formats.csv"


@pytest.fixture
def event():
    def event_func(input_prefix, delimiter=None, chunksize=100, schema=None):
        event = {
            "execution_name": "boligpriser-UUID",
            "task": "csv_exporter",
            "payload": {
                "pipeline": {
                    "id": "husholdninger-med-barn",
                    "task_config": {
                        "csv_exporter": {
                            # TODO: This is deprecated, but still in use by
                            # some. Remove this from the test once all users
                            # have been updated.
                            "format": "parquet",
                            "delimiter": delimiter,
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


@pytest.fixture
def schema_wrong(s3_client):
    return lambda: single_input(s3_client, input_path_wrong_schema, "s3/prefix/")


@pytest.fixture
def schema(s3_client):
    return lambda: single_input(s3_client, input_path_schema, "s3/prefix/")


@pytest.fixture
def husholdninger_single(s3_client):
    return lambda: single_input(s3_client, input_path, "s3/prefix/")


@pytest.fixture
def husholdninger_multiple(s3_client):
    return lambda: multiple_inputs(s3_client, f"{pwd}/data/multiple/", "s3/prefix/")


@pytest.fixture
def dates_file(s3_client):
    return lambda: single_input(s3_client, input_path_dates, "s3/prefix/")


@pytest.fixture
def dates_file_year_too_early(s3_client):
    return lambda: single_input(
        s3_client, input_path_dates_year_too_early, "s3/prefix/"
    )


@pytest.fixture
def dates_file_year_too_late(s3_client):
    return lambda: single_input(s3_client, input_path_dates_year_too_late, "s3/prefix/")


@pytest.fixture
def dates_file_date_string_value(s3_client):
    return lambda: single_input(
        s3_client, input_path_dates_date_string_value, "s3/prefix/"
    )


@pytest.fixture
def dates_file_date_with_time(s3_client):
    return lambda: single_input(
        s3_client, input_path_dates_date_with_time, "s3/prefix/"
    )


@pytest.fixture
def dates_file_date_wrong(s3_client):
    return lambda: single_input(s3_client, input_path_dates_date_wrong, "s3/prefix/")


@pytest.fixture
def datetimes_file(s3_client):
    return lambda: single_input(s3_client, input_path_datetimes, "s3/prefix/")


@pytest.fixture
def datetimes_file_with_tz(s3_client):
    return lambda: single_input(s3_client, input_path_datetimes_with_tz, "s3/prefix/")


@pytest.fixture
def datetimes_file_without_time(s3_client):
    return lambda: single_input(
        s3_client, input_path_datetimes_without_time, "s3/prefix/"
    )


@pytest.fixture
def datetimes_file_mixed_formats(s3_client):
    return lambda: single_input(
        s3_client, input_path_datetimes_mixed_formats, "s3/prefix/"
    )


def single_input(client, test_file, input_prefix):
    client.create_bucket(
        Bucket=BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
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
