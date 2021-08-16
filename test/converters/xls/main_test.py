import os
import sys

from moto import mock_s3

from test.util import mock_aws_s3_client

CWD = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(CWD, ".."))

bucket = "test-bucket"

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


@mock_s3
def test_xls_to_csv():
    from okdata.pipeline.converters.xls.main import xls_to_csv

    s3_mock = mock_aws_s3_client(bucket)

    excel_path = os.path.join(CWD, "data", "simple.xlsx")
    upload_key = "raw/green/dataset-in/1/20181115/simple.xlsx"
    output_prefix = "intermediate/yellow/dataset-out/1/20200123/xls2csv/"
    download_key = output_prefix + "simple.csv"

    with open(excel_path, "rb") as f:
        s3_mock.put_object(Bucket=bucket, Key=upload_key, Body=f)

    response = xls_to_csv(event, 0)
    assert response["status"] == "OK"
    assert response["s3_input_prefixes"]["dataset-out"] == output_prefix

    content = (
        s3_mock.get_object(Bucket=bucket, Key=download_key)
        .get("Body")
        .read()
        .decode("utf-8")
    )
    # To avoid a failed test when running on Windows
    content = content.replace("\r", "")

    assert content == expected_content


@mock_s3
def test_xls_to_csv_multiple_files():
    from okdata.pipeline.converters.xls.main import xls_to_csv

    s3_mock = mock_aws_s3_client(bucket)

    excel_path = os.path.join(CWD, "data", "simple.xlsx")
    upload_prefix = "raw/green/dataset-in/1/20181115/"
    output_prefix = "intermediate/yellow/dataset-out/1/20200123/xls2csv/"

    for k in ["simple1.xlsx", "simple2.xlsx"]:
        with open(excel_path, "rb") as f:
            s3_mock.put_object(Bucket=bucket, Key=upload_prefix + k, Body=f)

    response = xls_to_csv(event, 0)
    assert response["status"] == "OK"
    assert response["s3_input_prefixes"]["dataset-out"] == output_prefix

    for filename in ["simple1.csv", "simple2.csv"]:
        k = output_prefix + filename
        content = (
            s3_mock.get_object(Bucket=bucket, Key=k).get("Body").read().decode("utf-8")
        )
        # To avoid a failed test when running on Windows
        content = content.replace("\r", "")

        assert content == expected_content


@mock_s3
def test_xls_to_csv_default_config():
    from okdata.pipeline.converters.xls.main import xls_to_csv

    s3_mock = mock_aws_s3_client(bucket)

    excel_path = os.path.join(CWD, "data", "simple.xlsx")
    upload_key = "raw/green/dataset-in/1/20181115/simple.xlsx"
    output_prefix = "intermediate/yellow/dataset-out/1/20200123/xls2csv/"
    download_key = output_prefix + "simple.csv"

    with open(excel_path, "rb") as f:
        s3_mock.put_object(Bucket=bucket, Key=upload_key, Body=f)

    response = xls_to_csv(event_default, 0)
    assert response["status"] == "OK"
    assert response["s3_input_prefixes"]["dataset-out"] == output_prefix

    content = (
        s3_mock.get_object(Bucket=bucket, Key=download_key)
        .get("Body")
        .read()
        .decode("utf-8")
    )
    # To avoid a failed test when running on Windows
    content = content.replace("\r", "")

    assert content == expected_content
