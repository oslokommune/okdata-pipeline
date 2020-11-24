from aws_xray_sdk.core import xray_recorder

from okdata.aws.logging import log_add, logging_wrapper
from okdata.pipeline.converters.csv.types import OutputType
from okdata.pipeline.models import Config


@logging_wrapper("csv-exporter")
@xray_recorder.capture("export")
def export(event, context=None):
    format = event.get("format")
    if format is None:
        config = Config.from_lambda_event(event)
        format = config.task_config["format"]
    log_add(format=format)
    try:
        exporter = OutputType[format].value
    except KeyError:
        raise NotImplementedError

    output_path = exporter(event).export()
    return output_path
