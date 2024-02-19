from dataclasses import asdict

from deltalake import write_deltalake
from pandas.errors import OutOfBoundsDatetime

from okdata.aws.logging import log_add
from okdata.pipeline.models import StepData
from okdata.pipeline.converters.csv.base import BUCKET, Exporter


class DeltaExporter(Exporter):
    @staticmethod
    def _export(source, schema, out_prefix):
        df = Exporter.set_date_columns_on_dataframe(source, schema)
        write_deltalake(out_prefix, df)
        return out_prefix

    def export(self):
        inputs = self.read_csv()
        outputs = []
        errors = []
        s3_prefix = self.config.payload.output_dataset.s3_prefix.replace(
            "%stage%", "intermediate"
        )

        try:
            for filename, source in inputs:
                out_prefix = f"s3://{BUCKET}/{s3_prefix}"
                outputs.append(
                    self._export(source, self.task_config.schema, out_prefix)
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

        log_add(deltafiles=outputs)
        return asdict(
            StepData(
                status="CONVERSION_SUCCESS",
                errors=[],
                s3_input_prefixes={self.config.payload.output_dataset.id: s3_prefix},
            )
        )
