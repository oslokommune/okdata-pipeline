import os
import tempfile

import boto3
import awswrangler as wr

from okdata.pipeline.converters.base import BUCKET, Exporter
from okdata.pipeline.converters.xls.TableConverter import TableConverter


def convert_to_csv(xlsx_input, output, config):
    s3 = boto3.resource("s3", region_name=os.environ["AWS_REGION"])

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmpfile:
        s3.Bucket(BUCKET).download_file(xlsx_input, tmpfile.name)

        conv = TableConverter(config)
        wb = conv.read_excel_table(tmpfile.name)
        df = conv.convert_table(wb)

        # Drop unnamed columns; these typically appear because of some
        # unintended whitespace in cells and cause us trouble later if not
        # removed.
        unnamed_cols = [col for col in df.columns if col.startswith("Unnamed:")]
        df = df.drop(columns=unnamed_cols)

        csv = df.to_csv(sep=";", index=False)
        s3.Object(BUCKET, output).put(
            Body=csv, ContentType="text/csv", ContentEncoding="utf-8"
        )


class DeltaExporter(Exporter):
    def _export(self, source, out_prefix):
        wr.s3.to_deltalake(df=source, path=out_prefix, s3_allow_unsafe_rename=True)
        return out_prefix

    def export(self):
        inputs = self.read_xlsx()
        outputs = []
        errors = []
        s3_prefix = self.config.payload.output_dataset.s3_prefix.replace(
            "%stage%", "intermediate"
        )
        try:
            for filename, source in inputs:
                out_prefix = f"s3://{BUCKET}/{s3_prefix}"
                outputs.append(self._export(source, out_prefix))
        except ValueError as e:
            errors.append({"error": "ValueError", "message": str(e)})

        return self.export_response(s3_prefix, outputs, errors)
