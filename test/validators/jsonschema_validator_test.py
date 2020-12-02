import pytest

from okdata.pipeline.validators.csv.parser import parse_csv
from okdata.pipeline.validators.jsonschema_validator import JsonSchemaValidator


class TestValidJsonSchema:
    def test_incorrect_schema_version_throws_exception(
        self, dates_schema_unsupported_version
    ):
        with pytest.raises(ValueError):
            JsonSchemaValidator(dates_schema_unsupported_version).validate([])

    def test_correct_dates(self, dates_header, dates_schema):
        csv_data = parse_csv(
            [
                ["1", "2020", "2020-01-01", "2020-01-01T12:01:01"],
                ["1", "2020", "2020-01-01", "2020-01-01 12:01:01"],
                ["1", "2020", "2020-01-01", "2020-12-01T12-01"],
            ],
            dates_schema,
            header=dates_header,
        )
        validation_errors = JsonSchemaValidator(dates_schema).validate(csv_data)
        assert len(validation_errors) == 0

    def test_incorrect_date_colum(self, dates_header, dates_schema):
        csv_data = parse_csv(
            [["1", "2020", "garbish data", "2020-01-01T12:01:01"]],
            dates_schema,
            header=dates_header,
        )
        validation_errors = JsonSchemaValidator(dates_schema).validate(csv_data)
        assert len(validation_errors) == 1

    def test_valid_date_time_colum(self, dates_header, dates_schema):
        valid_dates = [
            "0009-12-01T12:01:01",
            "2020-12-12T12:01:01",
        ]
        for valid_date in valid_dates:
            csv_data = parse_csv(
                [["1", "2020", "2020-12-30", valid_date]],
                dates_schema,
                header=dates_header,
            )
            validation_errors = JsonSchemaValidator(dates_schema).validate(csv_data)
            assert len(validation_errors) == 0

    def test_incorrect_date_time_colum(self, dates_header, dates_schema):
        invalid_dates = [
            "2020-13-01T12:01:01",
            "2020 12 32T12:01:01",
            "garbish data",
        ]
        for invalid_date in invalid_dates:
            csv_data = parse_csv(
                [["1", "2020", "2020-12-30", invalid_date]],
                dates_schema,
                header=dates_header,
            )
            validation_errors = JsonSchemaValidator(dates_schema).validate(csv_data)
            assert len(validation_errors) == 1

    def test_valid_year_colum(self, dates_header, dates_schema):
        valid_years = ["2020", "-100", "9999"]
        for valid_year in valid_years:
            csv_data = parse_csv(
                [["1", valid_year, "2020-12-30", "2020-12-01T12:01:01"]],
                dates_schema,
                header=dates_header,
            )
            validation_errors = JsonSchemaValidator(dates_schema).validate(csv_data)
            assert len(validation_errors) == 0

    def test_incorrect_year_colum(self, dates_header, dates_schema):
        invalid_years = ["abc", ""]
        for invalid_year in invalid_years:
            csv_data = parse_csv(
                [["1", invalid_year, "2020-12-30", "2020-12-01T12:01:01"]],
                dates_schema,
                header=dates_header,
            )
            validation_errors = JsonSchemaValidator(dates_schema).validate(csv_data)
            assert len(validation_errors) == 1

    def test_validate_list_correct_values(self, json_schema):
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

        validation_errors = JsonSchemaValidator(json_schema).validate_list(json_data)
        assert len(validation_errors) == 0

    def test_validate_list_invalid_date_colum(self, json_schema):
        json_data = [
            {
                "id": "1",
                "year": "2020",
                "date": "garbish data",
                "datetime": "2020-01-01T12:01:01",
            }
        ]
        validation_errors = JsonSchemaValidator(json_schema).validate_list(json_data)
        assert len(validation_errors) == 1

    def test_validate_list_invalid_date_time_colum(self, json_schema):
        json_data = [
            {
                "id": "1",
                "year": "2020",
                "date": "2020-01-01",
                "datetime": "garbish data",
            }
        ]
        validation_errors = JsonSchemaValidator(json_schema).validate_list(json_data)
        assert len(validation_errors) == 1

    def test_validate_list_invalid_year_colum(self, json_schema):
        json_data = [
            {
                "id": "1",
                "year": "garbish data",
                "date": "2020-01-01",
                "datetime": "2020-01-01T12:01:01",
            }
        ]
        validation_errors = JsonSchemaValidator(json_schema).validate_list(json_data)
        assert len(validation_errors) == 1
