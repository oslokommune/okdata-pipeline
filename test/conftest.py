import pytest


@pytest.fixture
def test_event():
    return {
        "execution_name": "boligpriser-UUID",
        "task": "write_cleaned",
        "payload": {
            "pipeline": {
                "id": "husholdninger-med-barn",
                "task_config": {
                    "excel_to_csv": {
                        "pivot_column": "Barn i husholdningen",
                        "value_column": "Antall",
                    },
                    "validate_input": {"schema": "<json schema>"},
                    "write_cleaned": {"output_stage": "cleaned"},
                    "transform_csv": {
                        "delimiter": ";",
                        "header_row": True,
                        "csvlt": "<csvlt transformation>",
                    },
                    "validate_output": {"schema": "<json schema>"},
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
