import s3fs
from fastparquet import write

from okdata.pipeline.converters.csv.base import Exporter


class ParquetExporter(Exporter):
    def __init__(self, event):
        super().__init__(event)

    def export(self):
        df = self.read_csv()
        if self.input_type == "s3":
            s3 = s3fs.S3FileSystem()
            open_with = s3.open
            output_path = f"{Exporter.get_bucket()}/{self.output_value}"
            write(output_path, df, open_with=open_with)
        else:
            output_path = self.get_output_path()
            write(output_path, df)
        return output_path
