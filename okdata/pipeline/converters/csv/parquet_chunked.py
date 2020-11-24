import json
import logging
import os
import time

import pandas as pd
import s3fs
from fastparquet import write as parquet_write, writer as parquet_writer

from okdata.aws.logging import log_add, log_duration
from okdata.pipeline.converters.csv.base import Exporter, JSONSCHEMA_TO_DTYPE_MAP

log = logging.getLogger()

DEFAULT_LINES_PER_CHUNK = 1000000
JSONSCHEMA_TO_PARQUET_MAP = {"string": "utf8", "integer": "int", "boolean": "bool"}

# integer to float64 mapping MUST be preserved, unless you want to parse each individual
# line in csv input and check for N/A values on the line.
# Currently this must be done since we are doing a dynamic system where we don't really know
# what is coming in to our system

# Read csv file in X-lines per chunk and write a parquet part file
# per chunk
# See README.md for analysis of chunk size and approx file/line sizes supported
# by running in a lambda environment


class ChunkedParquetExporter(Exporter):
    def __init__(self, event):
        super().__init__(event)
        lines = os.getenv("PARQUET_LINES_PER_CHUNK", DEFAULT_LINES_PER_CHUNK)
        self.lines_per_chunk = int(event.get("linesperchunk", lines))
        log_add(lines_per_chunk=self.lines_per_chunk)
        self.compression = "infer"

    def open_with(self):
        open_with = None
        if self.input_type == "s3":
            fs = s3fs.S3FileSystem(anon=False)
            open_with = fs.open
        return open_with

    def get_output_schema(self):
        """
        Resolve schema from pipeline configuration

        Configuration for schema: pipeline-config/data/schemas/{my-schema} - configured for a pipeline in
        pipeline-config/pipeline_instances/{my-instance}/pipeline_instance.json
            "schemaId": "{my-schema}"

        If no schema is set the default ('infer') schema will be used
        {
          "type": "object",
          "properties": {
            "first_column": {
              "type": "string"
            },
            "second_column": {
              "type": "integer"
            },
            "third_column": {
              "type": "boolean"
            }
          }
        }

        Returns:
            schema: default to 'infer' if not found in event.config.schema
        """
        default_schema = None
        if not self.event.get("config"):
            return default_schema

        schema = self.event["config"].get("schema", None)
        if schema is None:
            return default_schema

        # schema["schema"] is the pipeline-config/data/schemas/{my-schema} configuration
        if isinstance(schema["schema"], dict):
            schema = schema["schema"]
        else:
            schema = json.loads(schema["schema"])

        log_add(schema=schema)
        ret = {}
        for k in schema["properties"]:
            type = schema["properties"][k]["type"]
            ret[k] = JSONSCHEMA_TO_PARQUET_MAP.get(type, "utf8")
        return ret

    def get_output_schema_from_input(self, input):
        """
        Get a schema dict for output based on the first line of the input file

        This is done to prevent potential exceptions thrown from writing data when
        data types are wrong AND we don't have a user-supplied schema for the current file
        """
        line = pd.read_csv(input, compression=self.compression, chunksize=1)
        ret = {}
        columns = line.get_chunk(0).columns
        for column in columns:
            ret[column] = "utf8"
        return ret

    def get_dtype(self):
        default_schema = None
        if not self.event.get("config"):
            return default_schema

        schema = self.event["config"].get("schema", None)
        if schema is None:
            return default_schema

        # schema["schema"] is the pipeline-config/data/schemas/{my-schema} configuration
        if isinstance(schema["schema"], dict):
            schema = schema["schema"]
        else:
            schema = json.loads(schema["schema"])

        log_add(schema=schema)
        ret = {}
        for k in schema["properties"]:
            type = schema["properties"][k]["type"]
            ret[k] = JSONSCHEMA_TO_DTYPE_MAP.get(type, "object")
        return ret

    def get_dtype_from_input(self, input):
        """
        Get a dtype dict of the input file based on the first line
        in the file. We set each column to dtype=object (see: https://pbpython.com/pandas_dtypes.html)

        This is done to prevent exceptions thrown from reading data when there are N/A values
        that we don't know before we are reading the user-supplied file AND we don't have
        a user-supplied schema for the current file
        """
        line = pd.read_csv(input, compression=self.compression, chunksize=1)
        ret = {}
        columns = line.get_chunk(0).columns
        for column in columns:
            ret[column] = "object"
        return ret

    def write(self, file_path, chunk, open_with, chunk_info, object_encoding):
        log_add(chunk_info=chunk_info)
        log_add(write_file_path=file_path)
        log_add(object_encoding=object_encoding)
        try:
            if open_with:
                log_duration(
                    lambda: parquet_write(
                        file_path,
                        chunk,
                        open_with=open_with,
                        object_encoding=object_encoding,
                    ),
                    f"write_to_s3_{chunk_info['counter']}_duration_ms",
                )
            else:
                log_duration(
                    lambda: parquet_write(
                        file_path, chunk, object_encoding=object_encoding
                    ),
                    f"write_to_s3_{chunk_info['counter']}_duration_ms",
                )
        except Exception as e:
            # TODO: log this to status and set FAILED - keep log_add for now to track it
            # Right now it will be logged as a permission error in the outermost handler
            log_add(chunk_info_failed=chunk_info)
            raise e

    def merge(self, filelist, open_with):
        log_add(merge_filelist=filelist)
        if open_with:
            log_duration(
                lambda: parquet_writer.merge(filelist, open_with=open_with),
                "write_merge_filelist_duration_ms",
            )
        else:
            log_duration(
                lambda: parquet_writer.merge(filelist),
                "write_merge_filelist_duration_ms",
            )

    def get_chunk_info(self, counter, input, output):
        ret = {
            "counter": counter,
            "chunksize": self.lines_per_chunk,
            "linerange": {
                "start": (counter - 1) * self.lines_per_chunk,
                "end": counter * self.lines_per_chunk,
            },
            "input": input,
            "output": output,
        }
        return ret

    def export(self):
        input = self.get_input_path()
        output = self.get_output_path()
        open_with = self.open_with()
        filelist = []
        counter = 1
        log_add(input_path=input)
        log_add(output_path=output)

        start_time = time.time()

        object_encoding = self.get_output_schema()
        if object_encoding is None:
            object_encoding = self.get_output_schema_from_input(input)

        dtype = self.get_dtype()
        if dtype is None:
            dtype = self.get_dtype_from_input(input)

        for chunk in pd.read_csv(
            input,
            compression=self.compression,
            chunksize=self.lines_per_chunk,
            dtype=dtype,
        ):
            chunk_info = self.get_chunk_info(counter, input, output)
            file_path = f"{output}/part.%i.parq" % counter
            filelist.append(file_path)
            self.write(file_path, chunk, open_with, chunk_info, object_encoding)
            counter += 1

        self.merge(filelist, open_with)
        end_time = time.time()
        total_time = end_time - start_time
        log_add(total_time=total_time)
        log_add(chunks=counter)
        return output
