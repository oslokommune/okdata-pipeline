import json

import okdata.pipeline.validators.csv as csv_validator
from okdata.pipeline.validators.csv.validator import StepConfig, validate_csv


def test_config():
    c = StepConfig('{"hello":"ok"}', False, ";", "'")
    assert type(c.schema) == dict


def test_config_from_event(event):
    task_config = event["payload"]["pipeline"]["task_config"]["validate_input"]
    c = StepConfig.from_task_config(task_config)
    assert type(c.schema) == dict
    assert c.schema == json.loads(task_config["schema"])


def csv_generator(*args):
    for element in [
        "int,bool,str,nil",
        "1,false,string,null",
        "2,false,string,null",
        *args,
    ]:
        yield element


def test_csv_validator(mocker, event):
    s3 = mocker.patch.object(csv_validator.validator, "s3")
    s3.list_objects_v2.return_value = {"Contents": [{"Key": "s3/key"}]}
    string_reader = mocker.patch.object(csv_validator.validator, "string_reader")
    string_reader.from_response.return_value = csv_generator()
    result = validate_csv(event, {})
    assert len(result["errors"]) == 0


def test_csv_validator_errors(mocker, event):
    s3 = mocker.patch.object(csv_validator.validator, "s3")
    s3.list_objects_v2.return_value = {"Contents": [{"Key": "s3/key"}]}
    string_reader = mocker.patch.object(csv_validator.validator, "string_reader")
    string_reader.from_response.return_value = csv_generator(
        "2,2,string,null", "2,true,string,bleh"
    )
    try:
        validate_csv(event, {})
    except Exception as e:
        error_list = e.args[0]
        assert len(error_list) == 2
