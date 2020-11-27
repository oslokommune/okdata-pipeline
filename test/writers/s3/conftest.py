import pytest


@pytest.fixture
def test_event():
    return {
        "execution_name": "boligpriser-UUID",
        "task": "write_cleaned",
        "payload": {
            "pipeline": {
                "id": "boligpriser",
                "task_config": {
                    "write_cleaned": {"output_stage": "cleaned"},
                    "write_processed": {"output_stage": "processed"},
                },
            },
            "output_dataset": {
                "id": "boligpriser",
                "version": "1",
                "edition": "20200120T133701",
                "s3_prefix": "%stage%/green/boligpriser/version=1/edition=20200120T133701/",
            },
            "step_data": {
                "s3_input_prefixes": {
                    "boligpriser": "raw/green/boligpriser/version=1/edition=20200120T133700/"
                },
                "status": "OK",
                "errors": [],
            },
        },
    }
