import boto3
import pandas as pd
import pytest
from moto import mock_s3

from okdata.pipeline.converters.csv.base import BUCKET, Exporter
from okdata.pipeline.converters.csv.handler import export
from okdata.pipeline.converters.csv.parquet import ParquetExporter
from test.converters.csv.conftest import input_path, output_path

event = {
    "format": "parquet",
    "output": {"type": "local", "value": output_path},
    "input": {"type": "local", "value": input_path},
}


def test_get_self_dict():
    p_export = ParquetExporter(event.copy())
    assert p_export.__dict__["delimiter"] == ","
    assert p_export.__dict__["output_value"] == event["output"]["value"]


def test_wrong_format():
    wrong_format = event.copy()
    wrong_format["format"] = "can you send this as en email please"
    with pytest.raises(NotImplementedError):
        export(wrong_format, {})


def test_missing_parameter():
    invalid_event = {
        "format": "parquet",
        "input": {"type": "local", "value": input_path},
    }
    with pytest.raises(KeyError):
        export(invalid_event, {})


def test_local_format(cleanup):
    original = pd.read_csv(input_path)
    result = export(event, {})
    df = pd.read_parquet(result)
    assert df.equals(original)


def test_delimiter():
    assert "," == Exporter.get_delimiter({})

    event = {"delimiter": "tab"}
    assert "\t" == Exporter.get_delimiter(event)

    event["delimiter"] = ";"
    assert ";" == Exporter.get_delimiter(event)


def test_get_bucket():
    assert Exporter.get_bucket() == BUCKET


@mock_s3
def test_s3_format():
    s3_event = {
        "format": "parquet",
        "output": {"type": "s3", "value": output_path},
        "input": {"type": "s3", "value": "s3_prefix/input"},
    }

    s3 = boto3.client("s3", "eu-west-1")
    s3.create_bucket(
        Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
    )
    with open(input_path, "r") as f:
        s3.put_object(Bucket=BUCKET, Key="s3_prefix/input", Body=f.read())

    original = pd.read_csv(input_path)
    export(s3_event, {})
    df = pd.read_parquet(f"s3://{BUCKET}/{output_path}")
    assert df.equals(original)
