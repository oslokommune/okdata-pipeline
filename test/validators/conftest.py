import json
import os
from pathlib import Path

import pytest


@pytest.fixture
def csv_data_dir():
    return Path(os.path.dirname(__file__), "csv/data")


@pytest.fixture
def dates_schema_unsupported_version(csv_data_dir):
    return json.loads(
        (csv_data_dir / "dates-schema-unsupported-version.json").read_text()
    )


@pytest.fixture
def dates_header(csv_data_dir):
    return json.loads((csv_data_dir / "dates-header.txt").read_text())


@pytest.fixture
def dates_schema(csv_data_dir):
    return json.loads((csv_data_dir / "dates-schema.json").read_text())


@pytest.fixture
def json_data_dir():
    return Path(os.path.dirname(__file__), "json/data")


@pytest.fixture
def json_schema(json_data_dir):
    return json.loads((json_data_dir / "schema.json").read_text())
