from dataclasses import asdict

from pandas.errors import OutOfBoundsDatetime

from okdata.aws.logging import log_add
from okdata.pipeline.models import StepData
from okdata.pipeline.converters.csv.base import BUCKET, NgExporter


class ParquetExporterNg(NgExporter):
    def s3_prefix(self):
        s3_prefix = (
            self.config.payload.output_dataset.s3_prefix.replace(
                "%stage%", "intermediate"
            )
            + self.config.task
            + "/"
        )
        return s3_prefix

    def export(self):
        inputs = self.read_csv()
        s3_prefix = self.s3_prefix()
        outputs = []
        schema = self.task_config.schema
        errors = []
        try:
            for filename, source in inputs:
                out_prefix = f"s3://{BUCKET}/" + s3_prefix + filename
                if self.task_config.chunksize is None:
                    outfile = f"{out_prefix}.parquet.gz"
                    outputs.append(outfile)
                    df = NgExporter.set_date_columns_on_dataframe(source, schema)
                    df.to_parquet(
                        outfile, engine="fastparquet", compression="gzip", times="int96"
                    )
                else:
                    for i, df in enumerate(source):
                        df = NgExporter.set_date_columns_on_dataframe(df, schema)
                        outfile = f"{out_prefix}.part.{i}.parquet.gz"
                        outputs.append(outfile)
                        df.to_parquet(
                            outfile,
                            engine="fastparquet",
                            compression="gzip",
                            times="int96",
                        )
        except OutOfBoundsDatetime as e:
            errors.append({"error": "OutOfBoundsDatetime", "message": str(e)})
        except ValueError as e:
            errors.append({"error": "ValueError", "message": str(e)})

        if len(errors) > 0:
            log_add(errors=errors)
            return asdict(
                StepData(
                    status="CONVERSION_FAILED",
                    errors=errors,
                    s3_input_prefixes={
                        self.config.payload.output_dataset.id: s3_prefix
                    },
                )
            )

        log_add(parquetfiles=outputs)
        return asdict(
            StepData(
                status="CONVERSION_SUCCESS",
                errors=[],
                s3_input_prefixes={self.config.payload.output_dataset.id: s3_prefix},
            )
        )
