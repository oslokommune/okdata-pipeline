import os
from pathlib import Path

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
def boligpriser_header():
    return ["delbydel_id", "navn", "pris", "til_salg"]


@fixture
def s3_object(s3_client, s3_bucket):
    def _s3_object(data):
        key = "intermediate/green/foo/bar/test.csv"
        s3_client.put_object(Bucket=s3_bucket, Key=key, Body=bytes(data, "utf-8"))
        return {"bucket": s3_bucket, "key": key}

    return _s3_object


@fixture
def s3_response(s3_client, s3_object):
    def _s3_response(data):
        res = s3_object(data)
        return s3_client.get_object(Bucket=res["bucket"], Key=res["key"])

    return _s3_response


@fixture
def event():
    e = {
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
        e["payload"]["pipeline"]["task_config"]["validate_input"]["schema"] = f.read()
    return e
