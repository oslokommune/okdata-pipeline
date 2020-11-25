from okdata.pipeline.validators.csv import string_reader

TEST_DATA = """delbydel_id;navn
0011;Lodalen
0012;Grønland
0013;Enerhaugen
0014;Nedre Tøyen
0015;Kampen
0016;Vålerenga
0017;Helsfyr
0021;Grünerløkka vest
0022;Grünerløkka øst
"""


def test_returns_strings(s3_response):
    reader = string_reader.from_response(s3_response(TEST_DATA))
    row = next(reader)
    assert type(row) is str


def test_returns_rows(s3_response):
    reader = string_reader.from_response(s3_response(TEST_DATA))
    rows = list(reader)
    assert rows == [
        "delbydel_id;navn",
        "0011;Lodalen",
        "0012;Grønland",
        "0013;Enerhaugen",
        "0014;Nedre Tøyen",
        "0015;Kampen",
        "0016;Vålerenga",
        "0017;Helsfyr",
        "0021;Grünerløkka vest",
        "0022;Grünerløkka øst",
    ]
