import pandas as pd
import pytest
import s3fs
from moto import mock_s3

from okdata.pipeline.converters.csv.base import BUCKET
from okdata.pipeline.converters.csv.exceptions import ConversionError
from okdata.pipeline.converters.csv.parquet import ParquetExporter

s3fs.S3FileSystem.cachable = False


@mock_s3
def test_ParquetExporter(event, husholdninger_single):
    (prefix, file) = husholdninger_single()
    event_data = event(prefix, chunksize=2)
    exporter = ParquetExporter(event_data)
    exporter.export()
    expected = pd.read_csv(file)
    output_prefix = event_data["payload"]["output_dataset"]["s3_prefix"].replace(
        "%stage%", "intermediate"
    )

    for i in range(4):
        result = pd.read_parquet(
            f"s3://{BUCKET}/"
            + output_prefix
            + event_data["task"]
            + f"/husholdninger.part.{i}.parquet.gz"
        )
        # result is a subset of expected
        assert len(result.merge(expected)) == len(result)


@mock_s3
def test_ParquetExporter_no_chunks(event, husholdninger_single):
    (prefix, file) = husholdninger_single()
    event_data = event(prefix, chunksize=None)
    exporter = ParquetExporter(event_data)
    exporter.export()
    expected = pd.read_csv(file)
    output_prefix = event_data["payload"]["output_dataset"]["s3_prefix"].replace(
        "%stage%", "intermediate"
    )

    result = pd.read_parquet(
        f"s3://{BUCKET}/"
        + output_prefix
        + event_data["task"]
        + "/husholdninger.parquet.gz"
    )
    # result is a subset of expected
    assert len(result.merge(expected)) == len(result)


# bytes|utf8|json|bson|bool|int|int32
SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["utf8", "check", "check2", "count", "date"],
    "properties": {
        "utf8": {"type": "string"},
        "check": {"type": "boolean"},
        "check2": {"type": "boolean"},
        "count": {"type": "number"},
        "date": {"type": "string", "format": "date"},
    },
}


@mock_s3
def test_ParquetExporter_with_schema(event, schema):
    (prefix, file) = schema()
    event_data = event(prefix, chunksize=None, schema=SCHEMA)
    exporter = ParquetExporter(event_data)
    exporter.export()

    output_prefix = event_data["payload"]["output_dataset"]["s3_prefix"].replace(
        "%stage%", "intermediate"
    )

    result = pd.read_parquet(
        f"s3://{BUCKET}/" + output_prefix + event_data["task"] + "/schema.parquet.gz"
    )
    assert list(result.dtypes)[1].name == "bool"
    assert list(result.dtypes)[2].name == "bool"
    assert list(result.dtypes)[3].name == "float64"
    assert list(result.dtypes)[4].name == "datetime64[ns]"

    assert list(result["date"])[0] == pd.Timestamp("2020-03-14")
    assert list(result["date"])[1] == pd.Timestamp("2020-01-01")
    assert pd.isnull(list(result["date"])[2])


@mock_s3
def test_ParquetExporter_with_schema_wrong_number(event, schema_wrong):
    (prefix, file) = schema_wrong()
    event_data = event(prefix, chunksize=None, schema=SCHEMA)
    exporter = ParquetExporter(event_data)
    with pytest.raises(
        ConversionError,
        match=".*cannot safely convert passed user dtype of bool for int64.*",
    ):
        exporter.export()


schema_dates = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["id", "year_column", "date_column"],
    "properties": {
        "id": {"type": "string"},
        "year_column": {"type": "string", "format": "year"},
        "date_column": {"type": "string", "format": "date"},
    },
}


def export_and_read_result(event_data, outputprefix):
    exporter = ParquetExporter(event_data)
    exporter.export()
    output_prefix = event_data["payload"]["output_dataset"]["s3_prefix"].replace(
        "%stage%", "intermediate"
    )
    fs = s3fs.core.S3FileSystem()
    result_path = f"{BUCKET}/{output_prefix}{event_data['task']}/{outputprefix}*"
    source_paths = fs.glob(path=result_path)

    result = pd.concat(
        pd.read_parquet(f"s3://{parquet_file}") for parquet_file in source_paths
    )
    return result


@mock_s3
def test_ParquetExporter_valid_dates_not_chunked(event, dates_file):
    (prefix, file) = dates_file()
    event_data = event(prefix, chunksize=None, schema=schema_dates)
    result = export_and_read_result(event_data, "schema_dates")
    assert result["year_column"][0].year == 1678
    assert result["year_column"][3].year == 2262
    assert result["date_column"][0].year == 1678
    assert result["date_column"][0].month == 10
    assert result["date_column"][2].year == 1978
    assert result["date_column"][2].month == 12


@mock_s3
def test_ParquetExporter_valid_dates_chunked(event, dates_file):
    (prefix, file) = dates_file()
    event_data = event(prefix, chunksize=2, schema=schema_dates)
    result = export_and_read_result(event_data, "schema_dates")
    assert result["year_column"][0].year == 1678
    assert result["year_column"][3].year == 2262
    assert result["date_column"][0].year == 1678
    assert result["date_column"][0].month == 10
    assert result["date_column"][2].year == 1978
    assert result["date_column"][2].month == 12


@mock_s3
def test_ParquetExporter_invalid_year_too_early_not_chunked(
    event, dates_file_year_too_early
):
    (prefix, file) = dates_file_year_too_early()
    event_data = event(prefix, chunksize=None, schema=schema_dates)
    exporter = ParquetExporter(event_data)
    result = exporter.export()
    assert result["status"] == "CONVERSION_FAILED"
    assert len(result["errors"]) == 1


@mock_s3
def test_ParquetExporter_invalid_year_too_early_chunked(
    event, dates_file_year_too_early
):
    (prefix, file) = dates_file_year_too_early()
    event_data = event(prefix, chunksize=1, schema=schema_dates)
    exporter = ParquetExporter(event_data)
    result = exporter.export()
    assert result["status"] == "CONVERSION_FAILED"
    assert len(result["errors"]) == 1


@mock_s3
def test_ParquetExporter_invalid_year_too_late_not_chunked(
    event, dates_file_year_too_late
):
    (prefix, file) = dates_file_year_too_late()
    event_data = event(prefix, chunksize=None, schema=schema_dates)
    exporter = ParquetExporter(event_data)
    result = exporter.export()
    assert result["status"] == "CONVERSION_FAILED"
    assert len(result["errors"]) == 1


@mock_s3
def test_ParquetExporter_invalid_year_too_late_chunked(event, dates_file_year_too_late):
    (prefix, file) = dates_file_year_too_late()
    event_data = event(prefix, chunksize=1, schema=schema_dates)
    exporter = ParquetExporter(event_data)
    result = exporter.export()
    assert result["status"] == "CONVERSION_FAILED"
    assert len(result["errors"]) == 1


@mock_s3
def test_ParquetExporter_date_with_string_not_chunked(
    event, dates_file_date_string_value
):
    (prefix, file) = dates_file_date_string_value()
    event_data = event(prefix, chunksize=None, schema=schema_dates)
    exporter = ParquetExporter(event_data)
    result = exporter.export()
    assert result["status"] == "CONVERSION_FAILED"
    assert len(result["errors"]) == 1


@mock_s3
def test_ParquetExporter_date_with_string_chunked(event, dates_file_date_string_value):
    (prefix, file) = dates_file_date_string_value()
    event_data = event(prefix, chunksize=1, schema=schema_dates)
    exporter = ParquetExporter(event_data)
    result = exporter.export()
    assert result["status"] == "CONVERSION_FAILED"
    assert len(result["errors"]) == 1


@mock_s3
def test_ParquetExporter_date_wrong_date_time_not_chunked(
    event, dates_file_date_time_wrong
):
    (prefix, file) = dates_file_date_time_wrong()
    event_data = event(prefix, chunksize=None, schema=schema_dates)
    exporter = ParquetExporter(event_data)
    result = exporter.export()
    assert result["status"] == "CONVERSION_FAILED"
    assert len(result["errors"]) == 1


@mock_s3
def test_ParquetExporter_date_time_not_chunked(event, dates_file_date_time):
    (prefix, file) = dates_file_date_time()
    event_data = event(prefix, chunksize=None, schema=schema_dates)
    result = export_and_read_result(event_data, "schema_dates_date_time")
    assert result["date_column"][0].year == 2020
    assert result["date_column"][0].month == 1
    assert result["date_column"][0].day == 1
    assert result["date_column"][0].hour == 12
    assert result["date_column"][0].minute == 12
    assert result["date_column"][0].second == 1
