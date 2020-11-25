from itertools import chain

import jsonschema

from okdata.aws.logging import log_add
from okdata.pipeline.validators.json.jsonschema_checkers import (
    jsonschema_datetime,
    jsonschema_year,
)

SCHEMA_SUPPORTED_VERSIONS = ["http://json-schema.org/draft-07/schema#"]
SCHEMA_FORMATTERS = {
    "http://json-schema.org/draft-07/schema#": jsonschema.draft7_format_checker
}


class JsonSchemaValidator:
    def __init__(self, schema, *args, **kwargs):
        self.validate_schema_version(schema)
        cls = jsonschema.validators.validator_for(schema)
        cls.check_schema(schema)
        self.validator = cls(schema, *args, **kwargs)
        self.validator.format_checker = self.format_checker(schema)

    def validate_schema_version(self, schema):
        schema_version = schema["$schema"]
        log_add(schema_version=schema_version)
        if schema_version not in SCHEMA_SUPPORTED_VERSIONS:
            raise ValueError(f"Schema version: {schema_version} is not supported")

    def format_checker(self, schema):
        if "$schema" not in schema:
            raise ValueError("Schema version not defined in schema")
        schema_version = schema["$schema"]
        if schema_version not in SCHEMA_FORMATTERS:
            raise ValueError(f"Could not find formatter for: {schema_version}")
        format_checker = SCHEMA_FORMATTERS[schema_version]

        @format_checker.checks("date-time")
        def check_date_time(value):
            """
            "date_time_column": {
                "type": "string",
                "format": "date-time"
            }
            """
            return jsonschema_datetime(value)

        @format_checker.checks("year")
        def check_year(value):
            """
            "year_column": {
                "type": "string",
                "format": "year"
            },
            """
            return jsonschema_year(value)

        return format_checker

    def validate(self, json_data: list):
        raw_errors = list(
            chain.from_iterable(
                [self.validator.iter_errors(data) for data in json_data]
            )
        )
        log_add(raw_errors=raw_errors)
        errors = []
        for e in raw_errors:
            index = e.path[0]
            if len(e.path) > 1:
                key = e.path[1]
                errors.append({"index": index, "key": key, "message": e.message})
            else:
                errors.append({"index": index, "message": e.message})

        return errors
