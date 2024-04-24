import awswrangler as wr
from pandas.errors import OutOfBoundsDatetime

from okdata.pipeline.converters.base import BUCKET, Exporter


class DeltaExporter(Exporter):
    @staticmethod
    def _export(source, schema, out_prefix):
        if schema:
            source = Exporter.set_date_columns_on_dataframe(source, schema)
        else:
            source = source.apply(Exporter.infer_column_dtype_from_input)

        wr.s3.to_deltalake(
            df=source,
            path=out_prefix,
            s3_allow_unsafe_rename=True,
        )

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

        return self.export_response(s3_prefix, outputs, errors)
