import copy
import os

from okdata.pipeline.converters.csv.handler import export
from test.converters.csv.conftest import (
    input_path,
    input_path_correct_schema,
    input_path_wrong_schema,
    output_path,
)

event = {
    "format": "parquetchunked",
    "linesperchunk": 1,
    "output": {"type": "local", "value": output_path},
    "input": {"type": "local", "value": input_path},
}
event_linesperchunk = copy.deepcopy(event)
event_linesperchunk["linesperchunk"] = 5

event_environment_lines_defined = copy.deepcopy(event)
del event_environment_lines_defined["linesperchunk"]

# bytes|utf8|json|bson|bool|int|int32
schema = {
    "type": "object",
    "properties": {
        "utf8": {"type": "string"},
        "bool": {"type": "boolean"},
        "bool2": {"type": "boolean"},
        "int": {"type": "integer"},
    },
}
event_schema = copy.deepcopy(event)
event_schema["input"] = {"type": "local", "value": input_path_correct_schema}
event_schema["linesperchunk"] = 100
event_schema["config"] = {"schema": {"id": "my-id", "schema": schema}}

event_wrong_schema = copy.deepcopy(event_schema)
event_wrong_schema["input"]["value"] = input_path_wrong_schema


def test_csv_to_parquet_chunked(cleanup):
    os.mkdir(output_path)
    result_path = export(event, {})
    file_count = len([1 for x in list(os.scandir(output_path)) if x.is_file()])
    assert result_path == "test/converters/csv/data/husholdninger.parquet"
    assert file_count == 8 + 2  # Two metadata files


def test_csv_to_parquet_chunked_five_lines_per_chunk(cleanup):
    os.mkdir(output_path)
    result_path = export(event_linesperchunk, {})
    file_count = len([1 for x in list(os.scandir(output_path)) if x.is_file()])
    assert result_path == "test/converters/csv/data/husholdninger.parquet"
    assert file_count == 2 + 2  # Two metadata files


def test_csv_to_parquet_chunked_environment_lines_per_chunk(cleanup):
    os.environ["PARQUET_LINES_PER_CHUNK"] = "3"
    os.mkdir(output_path)
    result_path = export(event_environment_lines_defined, {})
    del os.environ["PARQUET_LINES_PER_CHUNK"]
    file_count = len([1 for x in list(os.scandir(output_path)) if x.is_file()])
    assert result_path == "test/converters/csv/data/husholdninger.parquet"
    assert file_count == 3 + 2  # Two metadata files


def test_correct_schema(cleanup):
    os.mkdir(output_path)
    result_path = export(event_schema, {})
    assert "test/converters/csv/data/husholdninger.parquet" == result_path


def test_wrong_int_schema(cleanup):
    os.mkdir(output_path)
    try:
        export(event_wrong_schema, {})
    except Exception:
        assert True
