from aws_xray_sdk.core import xray_recorder
from okdata.aws.logging import logging_wrapper

from okdata.pipeline.converters.json.delta import DeltaExporter


@logging_wrapper("json-to-delta")
@xray_recorder.capture("json_to_delta")
def json_to_delta(event, context=None):
    return DeltaExporter(event).export()
