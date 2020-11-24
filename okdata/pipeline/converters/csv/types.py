from enum import Enum

from okdata.pipeline.converters.csv.parquet import ParquetExporter
from okdata.pipeline.converters.csv.parquet_chunked import ChunkedParquetExporter
from okdata.pipeline.converters.csv.parquet_ng import ParquetExporterNg


class OutputType(Enum):
    parquet = ParquetExporter
    parquetchunked = ChunkedParquetExporter
    parquetng = ParquetExporterNg
