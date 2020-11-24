import json

import boto3
from aws_xray_sdk.core import patch_all, xray_recorder

from okdata.aws.logging import log_add, logging_wrapper
from okdata.pipeline.models import Config

patch_all()

lambda_client = boto3.client("lambda", "eu-west-1")


@logging_wrapper("lambda-invoker")
@xray_recorder.capture("invoke")
def invoke(event, context):
    config = Config.from_lambda_event(event)
    function_arn = config.payload.pipeline.task_config.get(config.task).get("arn")

    log_add(function_arn=function_arn)
    log_add(event=event)

    response = lambda_client.invoke(
        FunctionName=function_arn,
        Payload=json.dumps(event),
        InvocationType="RequestResponse",
    )
    result = read_result(response)
    return result


class InvokedLambdaError(Exception):
    pass


def read_result(response):
    result = json.loads(response.get("Payload").read().decode("utf-8"))

    if "errorMessage" in result:
        raise InvokedLambdaError(result)

    return result
