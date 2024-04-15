from unittest.mock import ANY, patch

import awswrangler as wr
import pandas as pd
import pytest
import pytz

from okdata.pipeline.converters.base import BUCKET
from okdata.pipeline.converters.exceptions import ConversionError
from okdata.pipeline.converters.csv.parquet import ParquetExporter


def test_ParquetExporter_chunked(event, husholdninger_single):
    prefix, file = husholdninger_single()
    event_data = event(prefix, chunksize=2)
    exporter = ParquetExporter(event_data)
    output_prefix = event_data["payload"]["output_dataset"]["s3_prefix"].replace(
        "%stage%", "intermediate"
    )
    task = event_data["task"]

    with patch.object(exporter, "_parallel_export") as mocked_parallel_export:
        exporter.export()
        mocked_parallel_export.assert_called_once_with(
            "husholdninger",
            ANY,
            None,
            f"s3://{BUCKET}/{output_prefix}{task}/husholdninger",
        )


def test_ParquetExporter_no_chunks(event, husholdninger_single):
    (prefix, csv_file) = husholdninger_single()
    event_data = event(prefix, chunksize=None)
    exporter = ParquetExporter(event_data)
    exporter.export()
    expected = pd.read_csv(csv_file, sep=None, engine="python")
    output_prefix = event_data["payload"]["output_dataset"]["s3_prefix"].replace(
        "%stage%", "intermediate"
    )

    s3_prefix = output_prefix + event_data["task"] + "/husholdninger.parquet.gz"
    result = wr.s3.read_parquet(f"s3://{BUCKET}/{s3_prefix}")

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


def test_ParquetExporter_with_schema(event, schema):
    (prefix, file) = schema()
    event_data = event(prefix, chunksize=None, schema=SCHEMA)
    exporter = ParquetExporter(event_data)
    exporter.export()

    output_prefix = event_data["payload"]["output_dataset"]["s3_prefix"].replace(
        "%stage%", "intermediate"
    )

    result = wr.s3.read_parquet(
        f"s3://{BUCKET}/" + output_prefix + event_data["task"] + "/schema.parquet.gz",
        dtype_backend="pyarrow",
    )

    assert list(result.dtypes)[0].name == "string[pyarrow]"  # col: utf8
    assert list(result.dtypes)[1].name == "bool[pyarrow]"  # col: check
    assert list(result.dtypes)[2].name == "bool[pyarrow]"  # col: check2
    assert list(result.dtypes)[3].name == "double[pyarrow]"  # col: count
    assert list(result.dtypes)[4].name == "timestamp[ns][pyarrow]"  # col: date

    assert list(result["date"])[0] == pd.Timestamp("2020-03-14")
    assert list(result["date"])[1] == pd.Timestamp("2024-03-12")
    assert pd.isnull(list(result["date"])[2])


def test_ParquetExporter_with_schema_wrong_number(event, schema_wrong):
    (prefix, file) = schema_wrong()
    event_data = event(prefix, chunksize=None, schema=SCHEMA)
    exporter = ParquetExporter(event_data)
    with pytest.raises(
        ConversionError,
        match="^Cannot convert data according to JSON schema",
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

schema_datetimes = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["id", "datetime_column"],
    "properties": {
        "id": {"type": "string"},
        "datetime_column": {"type": "string", "format": "date-time"},
    },
}


def export_and_read_result(event_data, outputprefix):
    exporter = ParquetExporter(event_data)
    exporter.export()
    output_prefix = event_data["payload"]["output_dataset"]["s3_prefix"].replace(
        "%stage%", "intermediate"
    )

    source_paths = wr.s3.list_objects(f"s3://{BUCKET}/{output_prefix}*")
    return pd.concat(wr.s3.read_parquet(parquet_file) for parquet_file in source_paths)


def test_ParquetExporter_valid_dates(event, dates_file):
    (prefix, file) = dates_file()
    event_data = event(prefix, chunksize=None, schema=schema_dates)
    result = export_and_read_result(event_data, "schema_dates")
    assert result["year_column"][0].year == 1678
    assert result["year_column"][3].year == 2262
    assert result["date_column"][0].year == 1678
    assert result["date_column"][0].month == 10
    assert result["date_column"][2].year == 1978
    assert result["date_column"][2].month == 12


def test_ParquetExporter_valid_dates_no_schema(event, dates_file):
    (prefix, file) = dates_file()
    event_data = event(prefix, chunksize=None, schema=None)
    result = export_and_read_result(event_data, "schema_dates")

    assert result["date_column"][0].year == 1678
    assert result["date_column"][0].month == 10
    assert result["date_column"][2].year == 1978
    assert result["date_column"][2].month == 12


def test_ParquetExporter_invalid_year_too_early(event, dates_file_year_too_early):
    (prefix, file) = dates_file_year_too_early()
    event_data = event(prefix, chunksize=None, schema=schema_dates)
    exporter = ParquetExporter(event_data)
    result = exporter.export()
    assert result["status"] == "CONVERSION_FAILED"
    assert len(result["errors"]) == 1


def test_ParquetExporter_invalid_year_too_late(event, dates_file_year_too_late):
    (prefix, file) = dates_file_year_too_late()
    event_data = event(prefix, chunksize=None, schema=schema_dates)
    exporter = ParquetExporter(event_data)
    result = exporter.export()
    assert result["status"] == "CONVERSION_FAILED"
    assert len(result["errors"]) == 1


def test_ParquetExporter_date_with_string(event, dates_file_date_string_value):
    (prefix, file) = dates_file_date_string_value()
    event_data = event(prefix, chunksize=None, schema=schema_dates)
    exporter = ParquetExporter(event_data)
    result = exporter.export()
    assert result["status"] == "CONVERSION_FAILED"
    assert len(result["errors"]) == 1


def test_ParquetExporter_date_with_time(event, dates_file_date_with_time):
    (prefix, file) = dates_file_date_with_time()
    event_data = event(prefix, chunksize=None, schema=schema_dates)
    result = export_and_read_result(event_data, "schema_dates_date_with_time")
    assert result["date_column"][0].year == 2020
    assert result["date_column"][0].month == 1
    assert result["date_column"][0].day == 1


def test_ParquetExporter_date_wrong_date(event, dates_file_date_wrong):
    (prefix, file) = dates_file_date_wrong()
    event_data = event(prefix, chunksize=None, schema=schema_dates)
    exporter = ParquetExporter(event_data)
    result = exporter.export()
    assert result["status"] == "CONVERSION_FAILED"
    assert len(result["errors"]) == 1


def test_ParquetExporter_valid_datetimes(event, datetimes_file):
    (prefix, file) = datetimes_file()
    event_data = event(prefix, chunksize=None, schema=schema_datetimes)
    result = export_and_read_result(event_data, "schema_datetimes")

    assert result["datetime_column"][0].year == 1678
    assert result["datetime_column"][0].month == 10
    assert result["datetime_column"][0].day == 20
    assert result["datetime_column"][0].hour == 12
    assert result["datetime_column"][0].minute == 0
    assert result["datetime_column"][0].second == 1

    assert result["datetime_column"][2].year == 1978
    assert result["datetime_column"][2].month == 12
    assert result["datetime_column"][2].day == 31
    assert result["datetime_column"][2].hour == 20
    assert result["datetime_column"][2].minute == 23
    assert result["datetime_column"][2].second == 14


def test_ParquetExporter_valid_datetimes_no_schema(event, datetimes_file):
    (prefix, file) = datetimes_file()
    event_data = event(prefix, chunksize=None, schema=None)
    result = export_and_read_result(event_data, "datetimes")

    assert result["datetime_column"][0].year == 1678
    assert result["datetime_column"][0].month == 10
    assert result["datetime_column"][0].day == 20
    assert result["datetime_column"][0].hour == 12
    assert result["datetime_column"][0].minute == 0
    assert result["datetime_column"][0].second == 1

    assert result["datetime_column"][2].year == 1978
    assert result["datetime_column"][2].month == 12
    assert result["datetime_column"][2].day == 31
    assert result["datetime_column"][2].hour == 20
    assert result["datetime_column"][2].minute == 23
    assert result["datetime_column"][2].second == 14


def test_ParquetExporter_valid_datetimes_with_tz(event, datetimes_file_with_tz):
    (prefix, file) = datetimes_file_with_tz()
    event_data = event(prefix, chunksize=None, schema=None)
    result = export_and_read_result(event_data, "datetimes_with_tz")

    assert result["datetime_column"][0].year == 2021
    assert result["datetime_column"][0].month == 10
    assert result["datetime_column"][0].day == 20
    assert result["datetime_column"][0].hour == 12
    assert result["datetime_column"][0].minute == 0
    assert result["datetime_column"][0].second == 1
    assert result["datetime_column"][0].tz == pytz.UTC

    assert result["datetime_column"][2].year == 1989
    assert result["datetime_column"][2].month == 4
    assert result["datetime_column"][2].day == 21
    assert result["datetime_column"][2].hour == 11
    assert result["datetime_column"][2].minute == 58
    assert result["datetime_column"][2].second == 51
    assert result["datetime_column"][2].tz == pytz.UTC


def test_ParquetExporter_valid_datetimes_without_time(
    event, datetimes_file_without_time
):
    (prefix, file) = datetimes_file_without_time()
    event_data = event(prefix, chunksize=None, schema=None)
    result = export_and_read_result(event_data, "schema_datetimes_without_time")

    assert result["datetime_column"][0].year == 1678
    assert result["datetime_column"][0].month == 10
    assert result["datetime_column"][0].day == 20
    assert result["datetime_column"][0].hour == 0
    assert result["datetime_column"][0].minute == 0
    assert result["datetime_column"][0].second == 0

    assert result["datetime_column"][2].year == 1978
    assert result["datetime_column"][2].month == 12
    assert result["datetime_column"][2].day == 31
    assert result["datetime_column"][2].hour == 0
    assert result["datetime_column"][2].minute == 0
    assert result["datetime_column"][2].second == 0


def test_ParquetExporter_valid_datetimes_mixed_formats(
    event, datetimes_file_mixed_formats
):
    (prefix, file) = datetimes_file_mixed_formats()
    event_data = event(prefix, chunksize=None, schema=None)
    result = export_and_read_result(event_data, "schema_datetimes_mixed_formats")

    assert result["datetime_column"][0].year == 2024
    assert result["datetime_column"][0].month == 3
    assert result["datetime_column"][0].day == 13
    assert result["datetime_column"][0].hour == 20
    assert result["datetime_column"][0].minute == 0
    assert result["datetime_column"][0].second == 0

    assert result["datetime_column"][1].year == 1987
    assert result["datetime_column"][1].month == 8
    assert result["datetime_column"][1].day == 26
    assert result["datetime_column"][1].hour == 20
    assert result["datetime_column"][1].minute == 0
    assert result["datetime_column"][1].second == 0
