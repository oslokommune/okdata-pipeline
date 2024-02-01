import os

import openpyxl
import pandas as pd

from okdata.pipeline.converters.xls.TableConfig import TableConfig


class TableConverter:
    """Functions to read an XLSX file and convert it to a Pandas DataFrame."""

    def __init__(self, config):
        if not isinstance(config, TableConfig):
            raise TypeError("config must be of type TableConfig")

        self.config = config

    @staticmethod
    def read_excel_table(source_file):
        """Load an XLSX file from the file system and return a workbook."""

        if not isinstance(source_file, str):
            raise TypeError("source_file must be a string")

        if not os.path.splitext(source_file)[1] == ".xlsx":
            raise ValueError(f"source_file must end in .xlsx: {source_file}")

        return openpyxl.load_workbook(source_file)

    def convert_table(self, wb):
        """Convert an openpyxl workbook to a Pandas DataFrame.

        The conversion is based on the table conversion configuration. If the
        configuration contains multiple sub-table data sources, it will
        concatenate them together into a single DataFrame.
        """
        df = None

        num_subtables = len(self.config.table_sources)
        if num_subtables > 1 and not self.config.column_names:
            raise ValueError(
                "Reading multiple subtables requires setting column names explicitly"
            )

        for ts in self.config.table_sources:
            df_ts = self._extract_sub_table(wb, ts)
            df = df_ts.copy() if df is None else pd.concat((df, df_ts))

        return df

    def _extract_sub_table(self, wb, table_source):
        """Extract a sub-table based on the table source configuration.

        If the table conversion configuration defines an extra column, it will
        add this to the DataFrame and populate it using the defined extra data
        cell.
        """
        start_row = table_source.start_row - 1
        start_col = table_source.start_col - 1

        num_subtables = len(self.config.table_sources)

        if self.config.column_names:
            num_columns = len(self.config.column_names)
            cols = range(start_col, start_col + num_columns)
        else:
            cols = None

        header_arg = 0 if self.config.table_has_header else None

        if self.config.table_has_header and num_subtables == 1:
            names_arg = None
        else:
            names_arg = self.config.column_names

        df = pd.read_excel(
            io=wb,
            sheet_name=self.config.sheet_name,
            engine="openpyxl",
            header=header_arg,
            names=names_arg,
            usecols=cols,
            skiprows=start_row,
        )

        if self.config.table_has_header:
            self.check_column_names(df)

        if self.config.pivot_config:
            df = self.pivot_table(df)

        if self.config.extra_col:
            sheet = wb[self.config.sheet_name]
            value = sheet.cell(table_source.extra_row, table_source.extra_col).value
            value = self._to_dtype(value)
            df[self.config.extra_col.name] = value

        return self.filter_empty_rows(df)

    def _to_dtype(self, value):
        """Convert cell value `value` to a defined data type."""

        if self.config.extra_col.dtype is str:
            return str(value)
        elif self.config.extra_col.dtype is int:
            return int(value)
        elif self.config.extra_col.dtype is float:
            return float(value)
        raise TypeError("Extra column type is not str, int or float")

    def check_column_names(self, df):
        """Check whether a Pandas DataFrame matches the configured columns.

        Raise `ValueError` if not.
        """
        if self.config.column_names and not (
            list(df.columns) == self.config.column_names
        ):
            error_lines = [
                "Expected header did not match actual.",
                f"Expected: {self.config.column_names}",
                f"Actual: {list(df.columns)}",
            ]
            raise ValueError("\n".join(error_lines))

    def filter_empty_rows(self, df):
        """Filter away empty rows.

        This will happen if the bottom of the table is above the bottom of the
        sheet.
        """
        if self.config.column_names:
            return df[df[self.config.column_names[0]].notna()]

        return df

    def pivot_table(self, df):
        pivot_column = self.config.pivot_config.pivot_column
        value_column = self.config.pivot_config.value_column
        key_columns = [x for x in df if x not in [pivot_column, value_column]]
        df_pivot = pd.pivot_table(
            df,
            index=key_columns,
            columns=pivot_column,
            values=value_column,
            fill_value=self.config.pivot_config.fillna,
            aggfunc="sum",
        )
        return df_pivot.reset_index()
