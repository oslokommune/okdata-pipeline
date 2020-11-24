import copy

import pandas as pd

from okdata.pipeline.converters.csv.handler import export
from test.converters.csv.conftest import (
    input_path,
    input_path_gzip,
    input_path_tsv,
    input_path_tsv_gzip,
    output_path,
)

event = {
    "format": "parquet",
    "output": {"type": "local", "value": output_path},
    "input": {"type": "local", "value": input_path},
}
event_gzip = copy.deepcopy(event)
event_gzip["input"]["value"] = input_path_gzip

event_tsv = copy.deepcopy(event)
event_tsv["input"]["value"] = input_path_tsv
event_tsv["delimiter"] = "tab"

event_tsv_gzip = copy.deepcopy(event)
event_tsv_gzip["input"]["value"] = input_path_tsv_gzip
event_tsv_gzip["delimiter"] = "tab"


def test_csv_to_parquet(cleanup):
    original = pd.read_csv(input_path)
    result = export(event, {})
    df = pd.read_parquet(result)
    assert df.equals(original)


def test_gzip_csv_to_parquet(cleanup):
    original = pd.read_csv(input_path)
    result = export(event_gzip, {})
    df = pd.read_parquet(result)
    assert df.equals(original)


def test_tsv_to_parquet(cleanup):
    original = pd.read_csv(input_path_tsv, delimiter="\t")
    result = export(event_tsv, {})
    df = pd.read_parquet(result)
    assert df.equals(original)


def test_gzip_tsv_to_parquet(cleanup):
    original = pd.read_csv(input_path_tsv, delimiter="\t")
    result = export(event_tsv_gzip, {})
    df = pd.read_parquet(result)
    assert df.equals(original)
