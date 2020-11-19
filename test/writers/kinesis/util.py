import boto3

kinesis_conn = boto3.client("kinesis", region_name="eu-west-1")
s3_conn = boto3.resource("s3", region_name="eu-west-1")


def create_stream(stream_name):
    kinesis_conn = boto3.client("kinesis", region_name="eu-west-1")
    kinesis_conn.create_stream(StreamName=stream_name, ShardCount=1)
    return kinesis_conn


def get_records_from_stream(stream_name):
    kinesis_conn = boto3.client("kinesis", region_name="eu-west-1")
    stream_info = kinesis_conn.describe_stream(StreamName=stream_name)
    shard_id = stream_info["StreamDescription"]["Shards"][0]["ShardId"]
    shard_iterator = kinesis_conn.get_shard_iterator(
        StreamName=stream_name, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
    )

    return kinesis_conn.get_records(ShardIterator=shard_iterator["ShardIterator"])[
        "Records"
    ]
