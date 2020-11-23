import pytest

from okdata.pipeline.validators.json.jsonschema_validator import JsonSchemaValidator


class TestValidJsonSchema:
    def test_invalid_schema_version_throws_exception(self, schema_unsupported_version):
        with pytest.raises(ValueError):
            JsonSchemaValidator(schema_unsupported_version).validate([])

    def test_correct_values(self, schema):
        json_data = [
            {
                "id": "1",
                "year": "2020",
                "date": "2020-01-01",
                "datetime": "2020-01-01T12:01:01",
            },
            {
                "id": "1",
                "year": "2020",
                "date": "2020-01-01",
                "datetime": "2020-12-01T12-01",
            },
        ]

        validation_errors = JsonSchemaValidator(schema).validate(json_data)
        assert len(validation_errors) == 0

    def test_invalid_date_colum(self, schema):
        json_data = [
            {
                "id": "1",
                "year": "2020",
                "date": "garbish data",
                "datetime": "2020-01-01T12:01:01",
            }
        ]
        validation_errors = JsonSchemaValidator(schema).validate(json_data)
        assert len(validation_errors) == 1

    def test_invalid_date_time_colum(self, schema):
        json_data = [
            {
                "id": "1",
                "year": "2020",
                "date": "2020-01-01",
                "datetime": "garbish data",
            }
        ]
        validation_errors = JsonSchemaValidator(schema).validate(json_data)
        assert len(validation_errors) == 1

    def test_invalid_year_colum(self, schema):
        json_data = [
            {
                "id": "1",
                "year": "garbish data",
                "date": "2020-01-01",
                "datetime": "2020-01-01T12:01:01",
            }
        ]
        validation_errors = JsonSchemaValidator(schema).validate(json_data)
        assert len(validation_errors) == 1
