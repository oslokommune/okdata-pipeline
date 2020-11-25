import json

import pytest

from okdata.pipeline.validators.csv.parser import ParseErrors, parse_csv, parse_value


class TestValid(object):
    def test_parse_with_headers(self, boligpriser_schema, boligpriser_header):
        data = parse_csv(
            [
                ["001", "Østre byflak", "1010.01", "true"],
                ["002", "Hønse-Lovisaløkka", "5001,10", "false"],
            ],
            json.loads(boligpriser_schema),
            header=boligpriser_header,
        )
        assert data == [
            {
                "delbydel_id": "001",
                "navn": "Østre byflak",
                "pris": 1010.01,
                "til_salg": True,
            },
            {
                "delbydel_id": "002",
                "navn": "Hønse-Lovisaløkka",
                "pris": 5001.10,
                "til_salg": False,
            },
        ]

    def test_parse_no_headers(self, no_header_schema):
        data = parse_csv(
            [["120", "Foo", "true"], ["999199", "Bar", "false"]],
            json.loads(no_header_schema),
        )
        assert data == [
            {"0": 120, "1": "Foo", "2": True},
            {"0": 999_199, "1": "Bar", "2": False},
        ]

    def test_parse_empty_values(self, no_header_schema):
        data = parse_csv([["55", "", "true"]], json.loads(no_header_schema))
        assert data == [{"0": 55, "2": True}]


class TestInvalid(object):
    @pytest.mark.parametrize("test_input", ["nope", "true"])
    def test_wrong_floats(self, boligpriser_schema, boligpriser_header, test_input):
        with pytest.raises(ParseErrors) as e:
            parse_csv(
                [["0001", "Trøndelag", test_input]],
                json.loads(boligpriser_schema),
                header=boligpriser_header,
            )
            assert e.errors == [
                {
                    "row": 0,
                    "column": "pris",
                    "message": f"could not convert string to float: '{test_input}'",
                }
            ]

    @pytest.mark.parametrize("test_input", ["nope", "2.3", "true"])
    def test_wrong_ints(self, no_header_schema, test_input):
        with pytest.raises(ParseErrors) as e:
            parse_csv([[test_input, "Foo", "true"]], json.loads(no_header_schema))
            assert e.errors == [
                {
                    "row": 0,
                    "column": "0",
                    "message": f"invalid literal for int() with base 10: '{test_input}'",
                }
            ]


def test_simple_array():
    data = parse_csv([["1", "foo"], ["2", "bar"]], {"type": "array"})
    assert data == [["1", "foo"], ["2", "bar"]]


def test_empty_schema():
    data = parse_csv([["1", "foo"], ["2", "bar"]], {})
    assert data == [["1", "foo"], ["2", "bar"]]


class TestParseInt(object):
    def test_int(self):
        assert parse_value("2", "integer") == 2

    @pytest.mark.parametrize("test_input", ["not a number", "2.3", "1,5", "true", ""])
    def test_exception(self, test_input):
        with pytest.raises(ValueError):
            parse_value(test_input, "integer")


class TestParseNumber(object):
    @pytest.mark.parametrize(
        "test_input, expected",
        [("1.5", 1.5), ("201.2321", 201.2321), ("1,8", 1.8), ("0,854", 0.854)],
    )
    def test_float(self, test_input, expected):
        assert parse_value(test_input, "number") == expected

    def test_int(self):
        assert parse_value("120", "number") == 120

    @pytest.mark.parametrize("test_input", ["not a number", "true", ""])
    def test_exception(self, test_input):
        with pytest.raises(ValueError):
            parse_value(test_input, "number")


class TestParseBoolean(object):
    def test_true(self):
        assert parse_value("true", "boolean") is True

    def test_false(self):
        assert parse_value("false", "boolean") is False

    @pytest.mark.parametrize(
        "test_input", ["nope", "1", 1, "True", "False", "trueish", ""]
    )
    def test_not_boolean(self, test_input):
        with pytest.raises(ValueError):
            parse_value(test_input, "boolean")


class TestParseString(object):
    def test_number(self):
        assert parse_value("123", "string") == "123"

    def test_boolean(self):
        assert parse_value("true", "string") == "true"

    def test_emptystring(self):
        assert parse_value("", "string") == ""


class TestNull(object):
    def test_null(self):
        assert parse_value("null", "null") is None

    @pytest.mark.parametrize("test_input", ["nope", 1, 50.2, True, ""])
    def test_not_null(self, test_input):
        with pytest.raises(ValueError):
            parse_value(test_input, "null")
