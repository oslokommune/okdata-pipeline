from okdata.pipeline.writers.s3.models import S3Source

dataset_id = "some_dataset"
version = "1"
edition = "20200120T133701"

file_name_1 = "file1.json"
file_name_2 = "file2.json"

s3_input_prefix = f"raw/green/{dataset_id}/version={version}/edition={edition}/"
s3_output_prefix_processed = (
    f"processed/green/{dataset_id}/version={version}/edition={edition}/"
)
s3_output_prefix_cleaned = (
    f"cleaned/green/{dataset_id}/version={version}/edition={edition}/"
)
s3_output_prefix_processed_latest = (
    f"processed/green/{dataset_id}/version={version}/latest/"
)

filenames = [file_name_1, file_name_2]

s3_sources = [
    S3Source(
        filename=file_name_1,
        key=f"{s3_input_prefix}{file_name_1}",
    ),
    S3Source(
        filename=file_name_2,
        key=f"{s3_input_prefix}{file_name_2}",
    ),
]


def copy_event(output_stage, write_to_latest=False, edition=edition):
    return {
        "execution_name": "boligpriser-UUID",
        "task": "write_to_s3",
        "payload": {
            "pipeline": {
                "id": "boligpriser",
                "task_config": {
                    "write_to_s3": {
                        "output_stage": output_stage,
                        "write_to_latest": write_to_latest,
                        "content_type": "application/json",
                    },
                },
            },
            "output_dataset": {
                "id": dataset_id,
                "version": version,
                "edition": edition,
                "s3_prefix": f"%stage%/green/{dataset_id}/version={version}/edition={edition}/",
            },
            "step_data": {
                "s3_input_prefixes": {dataset_id: s3_input_prefix},
                "status": "OK",
                "errors": [],
            },
        },
    }
