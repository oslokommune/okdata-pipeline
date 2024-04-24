from okdata.pipeline.converters.base import Exporter


def test_export_response_success(test_event):
    exporter = Exporter(test_event)
    response = exporter.export_response("prefix", ["foo"], [])
    assert response["status"] == "CONVERSION_SUCCESS"
    assert response["errors"] == []
    assert response["s3_input_prefixes"] == {"boligpriser": "prefix"}


def test_export_response_failed(test_event):
    exporter = Exporter(test_event)
    response = exporter.export_response("prefix", ["foo"], ["err"])
    assert response["status"] == "CONVERSION_FAILED"
    assert response["errors"] == ["err"]
    assert response["s3_input_prefixes"] == {"boligpriser": "prefix"}
