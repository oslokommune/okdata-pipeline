from aws_xray_sdk.core import xray_recorder

from okdata.aws.logging import logging_wrapper
from okdata.pipeline.converters.csv.parquet import ParquetExporter


@logging_wrapper("csv-exporter")
@xray_recorder.capture("csv_to_parquet")
def csv_to_parquet(event, context=None):
    return ParquetExporter(event).export()
