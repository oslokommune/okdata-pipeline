import json
import os
from pathlib import Path

import pytest

test_data_directory = Path(os.path.dirname(__file__), "data")


@pytest.fixture
def schema():
    return json.loads((test_data_directory / "schema.json").read_text())


@pytest.fixture
def schema_unsupported_version():
    return json.loads(
        (test_data_directory / "schema_unsupported_version.json").read_text()
    )
