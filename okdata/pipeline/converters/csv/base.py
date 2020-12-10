import os
import re
from dataclasses import asdict, dataclass

import boto3
import pandas as pd

from okdata.aws.logging import log_add
from okdata.pipeline.converters.csv.exceptions import ConversionError
from okdata.pipeline.models import Config

BUCKET = os.environ["BUCKET_NAME"]
JSONSCHEMA_TO_DTYPE_MAP = {
    "string": "object",
    "integer": "float64",
    "boolean": "bool",
    "number": "float64",
}
DATE_FORMATS = ["date-time", "date", "year"]
DATE_FORMATS_INPUT_FORMAT = {
    "year": "%Y",
    "date": "%Y-%m-%d",
    "date-time": "%Y-%m-%dT%H:%M:%S",
}


class Exporter:
    def __init__(self, event):
        self.s3 = boto3.client("s3")
        self.s3fs_prefix = f"s3://{BUCKET}/"
        self.config = Config.from_lambda_event(event)
        self.task_config = TaskConfig.from_config(self.config)
        log_add(input_config=asdict(self.task_config))

    def _list_s3_objects(self):
        input_prefix = next(
            iter(self.config.payload.step_data.s3_input_prefixes.values())
        )

        return self.s3.list_objects_v2(Bucket=BUCKET, Prefix=input_prefix)["Contents"]

    @staticmethod
    def _read_csv_data(s3_key, delimiter, chunksize, dtype, date_columns=None):
        kwargs = {}
        if chunksize is not None:
            kwargs["chunksize"] = chunksize
        if dtype is not None:
            kwargs["dtype"] = dtype
        if date_columns is not None:
            kwargs["parse_dates"] = date_columns

        try:
            if s3_key.endswith(".gz"):
                df = pd.read_csv(
                    s3_key, compression="gzip", delimiter=delimiter, **kwargs
                )
            else:
                df = pd.read_csv(s3_key, delimiter=delimiter, **kwargs)
        except ValueError as ve:
            raise ConversionError(str(ve)) from ve
        return df

    @staticmethod
    def jsonschema_to_dtypes(schema):
        if schema is None:
            return None
        return {
            name: JSONSCHEMA_TO_DTYPE_MAP[prop["type"]]
            for name, prop in schema["properties"].items()
        }

    @staticmethod
    def get_dtype_from_input(input):
        """
        Get a dtype dict of the input file based on the first line
        in the file. We set each column to dtype=object (see: https://pbpython.com/pandas_dtypes.html)

        This is done to prevent exceptions thrown from reading data when there are N/A values
        that we don't know before we are reading the user-supplied file AND we don't have
        a user-supplied schema for the current file
        """
        line = pd.read_csv(input, compression="infer", chunksize=1)
        ret = {}
        columns = line.get_chunk(0).columns
        for column in columns:
            ret[column] = "object"
        return ret

    @staticmethod
    def get_dtype(schema, input):
        """
        Try to resolve the dtype for the columns for reading csv file
        Resolve dtype from the taskConfig.TASK_NAME.schema, if this is
        not available we read the first line (column headers) for the file that is about
        to be read in by pandas, and set each column to be of type object (default)
        """
        log_add(dtype_source="jsonschema")
        dtype = Exporter.jsonschema_to_dtypes(schema)
        if dtype is None:
            log_add(dtype_source=f"input:{input}")
            dtype = Exporter.get_dtype_from_input(input)
        log_add(dtype=dtype)
        return dtype

    @staticmethod
    def get_date_columns(schema):
        if not schema:
            return False
        return [
            name
            for name, prop in schema["properties"].items()
            if prop["type"] == "string" and prop.get("format") in DATE_FORMATS
        ]

    @staticmethod
    def remove_suffix(str):
        return re.sub(r"\.csv(\.gz)?$", "", str, flags=re.IGNORECASE)

    @staticmethod
    def get_convert_date_columns(schema):
        if not schema:
            return False
        return [
            {"name": name, "format": prop.get("format")}
            for name, prop in schema["properties"].items()
            if prop["type"] == "string" and prop.get("format") in DATE_FORMATS
        ]

    @staticmethod
    def set_date_columns_on_dataframe(df, schema):
        """
        Pandas have a date-range limitation from 1677 to 2262,
        see https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-timestamp-limits
        for more information on this.

        If content in reader now contains data that is NOT parsable by pandas the to_datetime() will
        throw a exception and the execution will stop

        to_datetime() will also throw ValueError if the date is invalid or not formatted
        according to the format found in DATE_FORMATS_INPUT_FORMAT
        """
        date_columns_convert = Exporter.get_convert_date_columns(schema)
        if date_columns_convert is False:
            return df
        for column in date_columns_convert:
            if column["format"] not in DATE_FORMATS_INPUT_FORMAT:
                raise KeyError(
                    f"""Date column: {column["name"]} defined but
                    could not find input format for it """
                )

            format = DATE_FORMATS_INPUT_FORMAT[column["format"]]
            df[column["name"]] = pd.to_datetime(df[column["name"]], format=format)
        return df

    def read_csv(self):
        s3_objects = self._list_s3_objects()
        schema = self.task_config.schema
        log_add(schema=schema)
        log_add(s3_keys=[obj["Key"] for obj in s3_objects])
        files = []
        for s3_object in s3_objects:
            key = self.s3fs_prefix + s3_object["Key"]
            dtype = Exporter.get_dtype(schema, key)
            df = self._read_csv_data(
                key,
                delimiter=self.task_config.delimiter,
                chunksize=self.task_config.chunksize,
                dtype=dtype,
            )
            filename = key.split("/")[-1]
            filename = Exporter.remove_suffix(filename)
            files.append((filename, df))
        return files

    def export(self):
        raise NotImplementedError


@dataclass
class TaskConfig(object):
    delimiter: str

    def __init__(self, chunksize=None, delimiter=None, schema=None):
        if delimiter == "tab":
            delimiter = "\t"
        elif delimiter is None:
            delimiter = ","
        self.chunksize = chunksize
        self.delimiter = delimiter
        self.schema = schema

    @classmethod
    def from_config(cls, config: Config):
        task_config = config.task_config
        return cls(
            chunksize=task_config.get("chunksize"),
            delimiter=task_config.get("delimiter"),
            schema=task_config.get("schema"),
        )
