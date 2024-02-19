from aws_xray_sdk.core import xray_recorder
from okdata.aws.logging import logging_wrapper

from okdata.pipeline.converters.csv.delta import DeltaExporter
from okdata.pipeline.converters.csv.parquet import ParquetExporter


@logging_wrapper("csv-to-parquet")
@xray_recorder.capture("csv_to_parquet")
def csv_to_parquet(event, context=None):
    return ParquetExporter(event).export()


@logging_wrapper("csv-to-delta")
@xray_recorder.capture("csv_to_delta")
def csv_to_delta(event, context=None):
    return DeltaExporter(event).export()
