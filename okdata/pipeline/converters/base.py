import os
import re
from dataclasses import asdict, dataclass

import awswrangler as wr
import boto3
import pandas as pd

from okdata.aws.logging import log_add
from okdata.pipeline.converters.exceptions import ConversionError
from okdata.pipeline.models import Config

BUCKET = os.environ["BUCKET_NAME"]
JSONSCHEMA_TO_DTYPE_MAP = {
    "string": "string[pyarrow]",
    "integer": "int64[pyarrow]",
    "boolean": "bool[pyarrow]",
    "number": "float64[pyarrow]",
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
    def _read_csv_data(s3_key, schema, delimiter, chunksize):
        # Note: awswrangler does not seem to pass the Pandas `delimiter`
        # parameter alias for `sep` to `pandas_kwargs`. When the latter is set
        # to `None`, Pandas automatically attempts to detect the separator
        # using Python’s builtin sniffer tool, csv.Sniffer.

        try:
            df = wr.s3.read_csv(
                path=s3_key,
                compression="gzip" if s3_key.endswith(".gz") else "infer",
                sep=delimiter,
                chunksize=chunksize if chunksize else None,
                dtype=Exporter.get_dtype(schema),
                dtype_backend="pyarrow",
                engine="python",
            )
        except ValueError as ve:
            raise ConversionError(str(ve)) from ve

        return df

    @staticmethod
    def _read_json_data(s3_key, chunksize):
        try:
            return wr.s3.read_json(
                path=s3_key,
                compression="gzip" if s3_key.endswith(".gz") else "infer",
                chunksize=chunksize if chunksize else None,
                dtype=False,
                dtype_backend="pyarrow",
            )
        except ValueError as ve:
            raise ConversionError(str(ve)) from ve

    @staticmethod
    def infer_column_dtype_from_input(col):
        # Check for date(time) type.
        if getattr(col.dtypes, "pyarrow_dtype", None) == "string":
            try:
                return pd.to_datetime(col, format="ISO8601")
            except ValueError:
                # Ignore errors and keep string type.
                pass

        # Default to string type for column with missing values.
        if getattr(col.dtypes, "pyarrow_dtype", None) in ("null", None):
            return col.astype(pd.StringDtype("pyarrow"))

        return col

    @staticmethod
    def get_dtype(schema=None):
        """
        Identify datatypes to apply to individual dataset columns based on
        provided `schema`.
        """

        if not schema:
            log_add(dtype=None, dtype_source="inferred")
            return None

        dtype = {
            name: JSONSCHEMA_TO_DTYPE_MAP[prop["type"]]
            for name, prop in schema["properties"].items()
        }
        log_add(dtype=dtype, dtype_source="jsonschema")
        return dtype

    @staticmethod
    def get_date_columns(schema):
        if not schema:
            return None

        date_columns = [
            name
            for name, prop in schema["properties"].items()
            if prop["type"] == "string" and prop.get("format") in DATE_FORMATS
        ]
        log_add(date_columns=date_columns)
        return date_columns

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

        If content in reader now contains data that is NOT parsable by pandas the `to_datetime()`
        will throw an exception and the execution will stop.

        `to_datetime()` will also throw `ValueError` if the date is invalid or not formatted
        according to the format found in `DATE_FORMATS_INPUT_FORMAT`.
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

            date_format = DATE_FORMATS_INPUT_FORMAT[column["format"]]

            df[column["name"]] = pd.to_datetime(
                df[column["name"]],
                format=date_format,
                # Allow the format to match anywhere in the target string.
                exact=False,
            )

        return df

    def read_csv(self):
        s3_objects = self._list_s3_objects()
        schema = self.task_config.schema
        delimiter = self.task_config.delimiter

        log_add(
            schema=schema,
            delimiter=schema,
            s3_keys=[obj["Key"] for obj in s3_objects],
        )

        files = []

        for s3_object in s3_objects:
            key = self.s3fs_prefix + s3_object["Key"]

            df = self._read_csv_data(
                key,
                schema=schema,
                delimiter=delimiter,
                chunksize=self.task_config.chunksize,
            )
            filename = key.split("/")[-1]
            filename = Exporter.remove_suffix(filename)
            files.append((filename, df))
        return files

    def read_json(self):
        s3_objects = self._list_s3_objects()
        log_add(s3_keys=[obj["Key"] for obj in s3_objects])

        files = []
        for s3_object in s3_objects:
            key = self.s3fs_prefix + s3_object["Key"]
            df = self._read_json_data(key, self.task_config.chunksize)
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
