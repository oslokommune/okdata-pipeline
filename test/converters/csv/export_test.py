import pandas as pd

from okdata.pipeline.converters.base import Exporter


def split_df(df):
    df1 = df.iloc[:10]
    df2 = df.iloc[10:]
    return df1, df2


def test_Exporter_read_csv_multiple_input(event, husholdninger_multiple):
    (input_prefix, files) = husholdninger_multiple()
    exporter = Exporter(event(input_prefix))
    result = exporter.read_csv()

    rl = sum([len(pd.concat(df)) for filename, df in result])
    df1 = pd.read_csv(files[0])
    df2 = pd.read_csv(files[1])
    expected = len(pd.concat([df1, df2]))
    assert rl == expected


def test_Exporter_read_csv_single_input(event, husholdninger_single):
    (input_prefix, file) = husholdninger_single()
    exporter = Exporter(event(input_prefix))
    result = exporter.read_csv()
    rl = sum([len(pd.concat(df)) for filename, df in result])
    expected = pd.read_csv(file)
    assert rl == len(expected)


def test_Exporter_remove_suffix():
    assert Exporter.remove_suffix("dates.csv") == "dates"
    assert Exporter.remove_suffix("dates.CSV") == "dates"
    assert Exporter.remove_suffix("dates.csv.gz") == "dates"
    assert Exporter.remove_suffix("dates.CSV.gz") == "dates"
    assert Exporter.remove_suffix("dates.csv.GZ") == "dates"
    assert Exporter.remove_suffix("dates.CSV.GZ") == "dates"
    assert Exporter.remove_suffix("dates") == "dates"
    assert Exporter.remove_suffix("date.csv") == "date"
    assert Exporter.remove_suffix("dates.csv.gz.csv.gz") == "dates.csv.gz"


def test_Exporter_get_convert_date_columns_empty_schema():
    assert Exporter.get_convert_date_columns(None) is False
    assert Exporter.get_convert_date_columns(False) is False
    assert Exporter.get_convert_date_columns("") is False


def test_Exporter_get_convert_date_columns():
    schema = {
        "properties": {
            "id": {"type": "string"},
            "year_column": {"type": "string", "format": "year"},
            "date_column": {"type": "string", "format": "date"},
        }
    }
    convert = Exporter.get_convert_date_columns(schema)
    assert convert[0]["name"] == "year_column"
    assert convert[0]["format"] == "year"
    assert convert[1]["name"] == "date_column"
    assert convert[1]["format"] == "date"
