import json

import okdata.pipeline.lambda_invoker as handler

test_event = {
    "execution_name": "uuid-test",
    "task": "lambda_invoker",
    "payload": {
        "pipeline": {
            "id": "innvandring-befolkning-lang-status",
            "task_config": {
                "write_to_processed": {"output_stage": "processed"},
                "lambda_invoker": {
                    "lambda_config": {"type": "lang_status"},
                    "arn": "bydelsfakta-data-processing-innvandrer-befolkning",
                },
            },
        },
        "output_dataset": {
            "id": "innvandring-befolkning-lang-status",
            "version": "1",
            "edition": "20200303T115117",
            "s3_prefix": "%stage%/yellow/bydelsfakta-grafdata/innvandring-befolkning-lang-status/version=1/edition=20200303T115117/",
        },
        "step_data": {
            "s3_input_prefixes": {
                "befolkning-etter-kjonn-og-alder": "processed/yellow/befolkning-etter-kjonn-og-alder/version=1/edition=20200303T104847/",
                "botid-ikke-vestlige": "processed/green/botid-ikke-vestlige/version=1/edition=20200207T094134/",
                "innvandrer-befolkningen-0-15-ar": "processed/green/innvandrer-befolkningen-0-15-ar/version=1/edition=20200211T125631/",
            },
            "status": "PENDING",
            "errors": [],
        },
    },
}


def test_invoke(mocker):
    client = mocker.patch.object(handler, "lambda_client")
    mocker.patch.object(
        handler,
        "read_result",
        return_value={
            "s3_input_prefixes": {
                "befolkning-lang-status": "processed/yellow/befolkning-lang-status/version=1/edition=20200303T999999/",
            },
            "status": "PENDING",
            "errors": [],
        },
    )
    handler.invoke(test_event, {})
    client.invoke.assert_called_with(
        FunctionName="bydelsfakta-data-processing-innvandrer-befolkning",
        Payload=json.dumps(test_event),
        InvocationType="RequestResponse",
    )
