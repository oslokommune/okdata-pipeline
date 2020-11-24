import json
import os
from pathlib import Path

import boto3
from moto import mock_s3
from pytest import fixture


@fixture
def data_dir(request):
    return Path(os.path.join(request.fspath.dirname), "data")


@fixture
def boligpriser_schema(data_dir):
    return (data_dir / "boligpriser-schema.json").read_text()


@fixture
def no_header_schema(data_dir):
    return (data_dir / "no-header-schema.json").read_text()


@fixture
def dates_header(data_dir):
    return json.loads((data_dir / "dates-header.txt").read_text())


@fixture
def dates_schema(data_dir):
    return json.loads((data_dir / "dates-schema.json").read_text())


@fixture
def dates_schema_unsupported_version(data_dir):
    return json.loads((data_dir / "dates-schema-unsupported-version.json").read_text())


@fixture
def boligpriser_header():
    return ["delbydel_id", "navn", "pris", "til_salg"]


@fixture
def s3():
    with mock_s3():
        boto3.DEFAULT_SESSION = None
        yield boto3.client("s3")


@fixture
def s3_bucket(s3):
    bucket = os.environ["BUCKET_NAME"]
    s3.create_bucket(
        Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
    )
    return bucket


@fixture
def s3_object(s3, s3_bucket):
    def _s3_object(data):
        key = "intermediate/green/foo/bar/test.csv"
        s3.put_object(Bucket=s3_bucket, Key=key, Body=bytes(data, "utf-8"))
        return {"bucket": s3_bucket, "key": key}

    return _s3_object


@fixture
def s3_response(s3, s3_object):
    def _s3_response(data):
        res = s3_object(data)
        return s3.get_object(Bucket=res["bucket"], Key=res["key"])

    return _s3_response


@fixture
def event():
    input = {
        "execution_name": "boligpriser-UUID",
        "task": "validate_input",
        "payload": {
            "pipeline": {
                "id": "husholdninger-med-barn",
                "task_config": {
                    "validate_input": {"schema": ""},
                    "validate_output": {"schema": "<json schema>"},
                },
            },
            "output_dataset": {
                "id": "boligpriser",
                "version": "1",
                "edition": "20200120T133701",
                "s3_prefix": "%stage%/green/boligpriser/version=1/edition=20200120T133701/",
            },
            "step_data": {
                "input_events": None,
                "s3_input_prefixes": {
                    "boligpriser": "raw/green/boligpriser/version=1/edition=20200120T133700/"
                },
                "status": "PENDING",
                "errors": [],
            },
        },
    }
    with open("test/validators/csv/data/test_schema.json", "r") as f:
        input["payload"]["pipeline"]["task_config"]["validate_input"][
            "schema"
        ] = f.read()
    return input
