import os

import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET_NAME"]


def read_s3_data(s3_input_prefixes: dict) -> list:
    prefix = next(iter(s3_input_prefixes.values()))

    objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    s3_path = next(iter(objects["Contents"]))["Key"]

    response = s3.get_object(Bucket=BUCKET, Key=s3_path)
    return [response["Body"].read().decode("utf-8")]
