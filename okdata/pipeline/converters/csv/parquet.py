from multiprocessing import Pipe, Process, connection

import awswrangler as wr
from pandas.errors import OutOfBoundsDatetime

from okdata.pipeline.converters.base import BUCKET, Exporter

# The maximum number of processes to run simultaneously when exporting in
# parallel. Set to match the number of vCPUs available in AWS Lambda, which is
# 6 as of 2021-04-15.
MAX_PROCESSES = 6


class ParquetExporter(Exporter):
    def s3_prefix(self):
        s3_prefix = (
            self.config.payload.output_dataset.s3_prefix.replace(
                "%stage%", "intermediate"
            )
            + self.config.task
            + "/"
        )
        return s3_prefix

    @staticmethod
    def _export(source, schema, out_prefix, part=None, connection=None):
        if schema:
            source = Exporter.set_date_columns_on_dataframe(source, schema)
        else:
            source = source.apply(Exporter.infer_column_dtype_from_input)

        outfile = "{}.{}parquet.gz".format(out_prefix, f"part.{part}." if part else "")

        wr.s3.to_parquet(source, outfile, compression="gzip")

        if connection:
            connection.send(outfile)

        return outfile

    def _parallel_export(self, filename, source, schema, out_prefix):
        # Unfortunately AWS Lambda doesn't support `multiprocessing.Pool`, so
        # we'll have to take care of the connections ourselves.
        connections = []

        for i, df in enumerate(source):
            parent_connection, child_connection = Pipe()
            connections.append(parent_connection)
            Process(
                target=self._export,
                args=(df, schema, out_prefix, i + 1, child_connection),
            ).start()

            if len(connections) >= MAX_PROCESSES:
                for c in connection.wait(connections):
                    yield c.recv()
                    connections.remove(c)

        while connections:
            for c in connection.wait(connections):
                yield c.recv()
                connections.remove(c)

    def export(self):
        inputs = self.read_csv()
        s3_prefix = self.s3_prefix()
        outputs = []
        schema = self.task_config.schema
        errors = []
        try:
            for filename, source in inputs:
                out_prefix = f"s3://{BUCKET}/{s3_prefix}{filename}"
                if self.task_config.chunksize:
                    outputs.extend(
                        self._parallel_export(filename, source, schema, out_prefix)
                    )
                else:
                    outputs.append(self._export(source, schema, out_prefix))
        except OutOfBoundsDatetime as e:
            errors.append({"error": "OutOfBoundsDatetime", "message": str(e)})
        except ValueError as e:
            errors.append({"error": "ValueError", "message": str(e)})

        return self.export_response(s3_prefix, outputs, errors)
