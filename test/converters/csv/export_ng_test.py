import moto
import pandas as pd
import s3fs

from okdata.pipeline.converters.csv.base import NgExporter

s3fs.S3FileSystem.cachable = False


def split_df(df):
    df1 = df.iloc[:10]
    df2 = df.iloc[10:]
    return df1, df2


@moto.mock_s3
def test_NgExporter_read_csv_multiple_input(event_ng, husholdninger_multiple):
    (input_prefix, files) = husholdninger_multiple()
    exporter = NgExporter(event_ng(input_prefix))
    result = exporter.read_csv()

    rl = sum([len(df.read()) for filename, df in result])
    df1 = pd.read_csv(files[0])
    df2 = pd.read_csv(files[1])
    expected = len(df1.append(df2))
    assert rl == expected


@moto.mock_s3
def test_NgExporter_read_csv_single_input(event_ng, husholdninger_single):
    (input_prefix, file) = husholdninger_single()
    exporter = NgExporter(event_ng(input_prefix))
    result = exporter.read_csv()
    rl = sum([len(df.read()) for filename, df in result])
    expected = pd.read_csv(file)
    assert rl == len(expected)


def test_NgExporter_remove_suffix():
    assert NgExporter.remove_suffix("dates.csv") == "dates"
    assert NgExporter.remove_suffix("dates.CSV") == "dates"
    assert NgExporter.remove_suffix("dates.csv.gz") == "dates"
    assert NgExporter.remove_suffix("dates.CSV.gz") == "dates"
    assert NgExporter.remove_suffix("dates.csv.GZ") == "dates"
    assert NgExporter.remove_suffix("dates.CSV.GZ") == "dates"
    assert NgExporter.remove_suffix("dates") == "dates"
    assert NgExporter.remove_suffix("date.csv") == "date"
    assert NgExporter.remove_suffix("dates.csv.gz.csv.gz") == "dates.csv.gz"


def test_NgExporter_get_convert_date_columns_empty_schema():
    assert NgExporter.get_convert_date_columns(None) is False
    assert NgExporter.get_convert_date_columns(False) is False
    assert NgExporter.get_convert_date_columns("") is False


def test_NgExporter_get_convert_date_columns():
    schema = {
        "properties": {
            "id": {"type": "string"},
            "year_column": {"type": "string", "format": "year"},
            "date_column": {"type": "string", "format": "date"},
        }
    }
    convert = NgExporter.get_convert_date_columns(schema)
    assert convert[0]["name"] == "year_column"
    assert convert[0]["format"] == "year"
    assert convert[1]["name"] == "date_column"
    assert convert[1]["format"] == "date"
