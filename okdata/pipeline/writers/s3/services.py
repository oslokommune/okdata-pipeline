import os

import boto3

from okdata.aws.logging import log_add, log_duration, log_exception
from okdata.pipeline.writers.s3.exceptions import IncompleteTransaction
from okdata.pipeline.writers.s3.models import S3Source


class S3Service:
    bucket = os.environ["BUCKET_NAME"]
    client = boto3.client("s3")

    def __init__(self):
        self.client = boto3.client("s3")
        log_add(s3_bucket=self.bucket)

    def copy(self, s3_sources, output_prefix, retries=3):
        failed_s3_sources = []
        for s3_source in s3_sources:
            try:
                self.client.copy_object(
                    Bucket=self.bucket,
                    Key=output_prefix + s3_source.filename,
                    CopySource={"Key": s3_source.key, "Bucket": self.bucket},
                )
            except Exception as e:
                failed_s3_sources.append(s3_source)
                log_exception(e)

        if len(failed_s3_sources) > 0:
            if retries > 0:
                self.copy(failed_s3_sources, output_prefix, retries - 1)
            else:
                raise IncompleteTransaction

    def delete_from_prefix(self, s3_prefix):
        objects_to_delete = [
            {"Key": obj["Key"]} for obj in self.list_objects_contents(s3_prefix)
        ]

        if not objects_to_delete:
            return

        self.client.delete_objects(
            Bucket=self.bucket,
            Delete={
                "Objects": [
                    {"Key": s3_object["Key"]} for s3_object in objects_to_delete
                ],
                "Quiet": True,
            },
        )
        log_add(deleted_from_s3_path=objects_to_delete)

    def resolve_s3_sources(self, source_prefix: str):
        source_objects = self.list_objects_contents(source_prefix)
        log_add(num_source_objects=len(source_objects))
        if not source_objects:
            raise Exception(f"No source files found at: {source_prefix}")

        s3_sources = []

        for obj in source_objects:
            source_key = obj["Key"]
            filename = source_key.removeprefix(source_prefix)
            s3_sources.append(S3Source(filename=filename, key=source_key))

        return s3_sources

    def list_objects_contents(self, s3_prefix):
        return log_duration(
            lambda: self._list_objects_contents(s3_prefix), "list_objects_v2_duration"
        )

    def _list_objects_contents(self, s3_prefix):
        s3_objects = self.client.list_objects_v2(Bucket=self.bucket, Prefix=s3_prefix)
        contents = s3_objects.get("Contents", [])
        is_truncated = s3_objects["IsTruncated"]
        while is_truncated:
            continuation_token = s3_objects["NextContinuationToken"]
            s3_objects = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=s3_prefix,
                ContinuationToken=continuation_token,
            )
            is_truncated = s3_objects["IsTruncated"]
            contents.extend(s3_objects.get("Contents", []))
        return contents
