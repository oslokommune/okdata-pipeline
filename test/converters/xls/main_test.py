import os
import sys

from okdata.pipeline.converters.xls.handlers import xlsx_to_csv

CWD = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(CWD, ".."))

event = {
    "execution_name": "dataset-uuid",
    "task": "xls2csv",
    "payload": {
        "pipeline": {
            "id": "instance-id",
            "task_config": {
                "xls2csv": {
                    "sheet_name": "Sheet1",
                    "table_has_header": True,
                    "column_names": ["A", "B"],
                    "table_sources": [{"start_row": 1, "start_col": 1}],
                }
            },
        },
        "output_dataset": {
            "id": "dataset-out",
            "version": "1",
            "edition": "20200123",
            "s3_prefix": "%stage%/yellow/dataset-out/1/20200123/",
        },
        "step_data": {
            "s3_input_prefixes": {"dataset-in": "raw/green/dataset-in/1/20181115/"},
            "status": "??",
            "errors": [],
        },
    },
}

event_default = {
    "execution_name": "dataset-uuid",
    "task": "xls2csv",
    "payload": {
        "pipeline": {"task_config": {}, "id": "instance-id"},
        "output_dataset": {
            "id": "dataset-out",
            "version": "1",
            "edition": "20200123",
            "s3_prefix": "%stage%/yellow/dataset-out/1/20200123/",
        },
        "step_data": {
            "s3_input_prefixes": {"dataset-in": "raw/green/dataset-in/1/20181115/"},
            "status": "??",
            "errors": [],
        },
    },
}

expected_content = "A;B\n" + "1;foo\n" + "2;bar\n" + "3;baz\n"


def test_xlsx_to_csv(s3_client, s3_bucket):
    excel_path = os.path.join(CWD, "data", "simple.xlsx")
    upload_key = "raw/green/dataset-in/1/20181115/simple.xlsx"
    output_prefix = "intermediate/yellow/dataset-out/1/20200123/xls2csv/"
    download_key = output_prefix + "simple.csv"

    with open(excel_path, "rb") as f:
        s3_client.put_object(Bucket=s3_bucket, Key=upload_key, Body=f)

    response = xlsx_to_csv(event, 0)
    assert response["status"] == "OK"
    assert response["s3_input_prefixes"]["dataset-out"] == output_prefix

    content = (
        s3_client.get_object(Bucket=s3_bucket, Key=download_key)
        .get("Body")
        .read()
        .decode("utf-8")
    )
    # To avoid a failed test when running on Windows
    content = content.replace("\r", "")

    assert content == expected_content


def test_xlsx_to_csv_multiple_files(s3_client, s3_bucket):
    excel_path = os.path.join(CWD, "data", "simple.xlsx")
    upload_prefix = "raw/green/dataset-in/1/20181115/"
    output_prefix = "intermediate/yellow/dataset-out/1/20200123/xls2csv/"

    for k in ["simple1.xlsx", "simple2.xlsx"]:
        with open(excel_path, "rb") as f:
            s3_client.put_object(Bucket=s3_bucket, Key=upload_prefix + k, Body=f)

    response = xlsx_to_csv(event, 0)
    assert response["status"] == "OK"
    assert response["s3_input_prefixes"]["dataset-out"] == output_prefix

    for filename in ["simple1.csv", "simple2.csv"]:
        k = output_prefix + filename
        content = (
            s3_client.get_object(Bucket=s3_bucket, Key=k)
            .get("Body")
            .read()
            .decode("utf-8")
        )
        # To avoid a failed test when running on Windows
        content = content.replace("\r", "")

        assert content == expected_content


def test_xlsx_to_csv_default_config(s3_client, s3_bucket):
    excel_path = os.path.join(CWD, "data", "simple.xlsx")
    upload_key = "raw/green/dataset-in/1/20181115/simple.xlsx"
    output_prefix = "intermediate/yellow/dataset-out/1/20200123/xls2csv/"
    download_key = output_prefix + "simple.csv"

    with open(excel_path, "rb") as f:
        s3_client.put_object(Bucket=s3_bucket, Key=upload_key, Body=f)

    response = xlsx_to_csv(event_default, 0)
    assert response["status"] == "OK"
    assert response["s3_input_prefixes"]["dataset-out"] == output_prefix

    content = (
        s3_client.get_object(Bucket=s3_bucket, Key=download_key)
        .get("Body")
        .read()
        .decode("utf-8")
    )
    # To avoid a failed test when running on Windows
    content = content.replace("\r", "")

    assert content == expected_content


def test_xlsx_to_csv_dirty_spreadsheet(s3_client, s3_bucket):
    excel_path = os.path.join(CWD, "data", "lav_inntekt_dirty.xlsx")
    upload_key = "raw/green/dataset-in/1/20181115/lav_inntekt_dirty.xlsx"
    output_prefix = "intermediate/yellow/dataset-out/1/20200123/xls2csv/"
    download_key = output_prefix + "lav_inntekt_dirty.csv"

    with open(excel_path, "rb") as f:
        s3_client.put_object(Bucket=s3_bucket, Key=upload_key, Body=f)

    del event["payload"]["pipeline"]["task_config"]
    response = xlsx_to_csv(event, None)
    assert response["status"] == "OK"
    assert response["s3_input_prefixes"]["dataset-out"] == output_prefix

    content = (
        s3_client.get_object(Bucket=s3_bucket, Key=download_key)
        .get("Body")
        .read()
        .decode("utf-8")
    )
    first_line = content.split("\n", 1)[0]
    headers = first_line.split(";")

    assert headers == [
        "År",
        "Bydel",
        "Delbydel",
        "Høyeste fullførte utdanning",
        "Antall personer",
    ]
