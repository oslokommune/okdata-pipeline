import pandas as pd
import xlrd

from okdata.pipeline.converters.xls.TableConfig import TableConfig


class TableConverter(object):
    """
    The TableConverter contains functionality to read an Excel file
    and convert this to a Pandas DataFrame, based on the TableConfig
    configuration.
    """

    def __init__(self, config):
        """
        Configures the TableConverter.

        Args:
            config (TableConfig): The table conversion configuration.
        """
        if not type(config) is TableConfig:
            raise TypeError("config must be of type TableConfig")

        self.config = config

    def read_excel_table(self, source_file):
        """
        Reads an Excel file from the file system.

        Args:
            source_file (str): File path to the Excel file.

        Returns:
            The xlrd workbook.
        """
        if not type(source_file) is str:
            raise TypeError("source_file must be a string")

        if not (
            source_file.lower()[-5:] == ".xlsx" or source_file.lower()[-4:] == ".xls"
        ):
            raise ValueError(
                "source_file must end in .xlsx or .xls: {filename}".format(
                    filename=source_file
                )
            )

        return xlrd.open_workbook(source_file)

    def convert_table(self, wb):
        """
        Converts an xlrd workbook to a Pandas DataFrame based on the
        table conversion configuration. If the configuration contains
        multiple sub-table data sources, it will concatenate them
        together into a single DataFrame.

        Args:
            wb (xlrd.book.Book): The xlrd workbook
        """
        df = None

        num_subtables = len(self.config.table_sources)
        if num_subtables > 1 and not self.config.column_names:
            raise ValueError(
                "Reading multiple subtables requires setting column names explicitly"
            )

        for ts in self.config.table_sources:
            df_ts = self.extract_sub_table(wb, ts)

            if df is None:
                df = df_ts.copy()
            else:
                df = pd.concat((df, df_ts))

        return df

    def extract_sub_table(self, wb, table_source):
        """
        Internal method to etract one sub-table based on the table source
        configuration.

        If the table conversion configuration defines an extra column, it
        will add this to the DataFrame and populate it using the defined
        extra data cell.
        """
        start_row = table_source.start_row - 1
        start_col = table_source.start_col - 1

        num_subtables = len(self.config.table_sources)

        if self.config.column_names:
            num_columns = len(self.config.column_names)
            cols = list(range(start_col, start_col + num_columns))
        else:
            cols = None

        header_arg = 0 if self.config.table_has_header else None

        if self.config.table_has_header and num_subtables == 1:
            names_arg = None
        else:
            names_arg = self.config.column_names

        try:
            df = pd.read_excel(
                io=wb,
                sheet_name=self.config.sheet_name,
                engine="xlrd",
                header=header_arg,
                names=names_arg,
                usecols=cols,
                skiprows=start_row,
            )

        except xlrd.biffh.XLRDError as e:
            raise OSError(e)

        if self.config.table_has_header:
            self.check_column_names(df)

        if self.config.pivot_config:
            df = self.pivot_table(df)

        if self.config.extra_col:
            sheet = wb.sheet_by_name(self.config.sheet_name)
            value = sheet.cell(
                table_source.extra_row - 1, table_source.extra_col - 1
            ).value
            value = self.to_dtype(value)
            df[self.config.extra_col.name] = value

        return self.filter_empty_rows(df)

    def to_dtype(self, value):
        """
        Internal method to convert the cell value to the defined data type.
        """
        if self.config.extra_col.dtype is str:
            return str(value)
        elif self.config.extra_col.dtype is int:
            return int(value)
        elif self.config.extra_col.dtype is float:
            return float(value)
        else:
            raise TypeError("Extra column type is not str, int or float")

    def check_column_names(self, df):
        """
        Checks whether a Pandas DataFrame matches the configured columns.
        """
        if self.config.column_names and not (
            list(df.columns) == self.config.column_names
        ):
            error_string = "Expected header did not match actual.\n"
            error_string += "Expected: " + str(self.config.column_names) + "\n"
            error_string += "Actual: " + str(list(df.columns))
            raise ValueError(error_string)

    def filter_empty_rows(self, df):
        """
        Filter away rows which do not have any content (this will happen
        if the bottom of the table is above the bottom of the sheet.)
        """

        if self.config.column_names:
            return df[df[self.config.column_names[0]].notna()]
        else:
            return df

    def pivot_table(self, df):
        pivot_column = self.config.pivot_config.pivot_column
        value_column = self.config.pivot_config.value_column
        key_columns = list(
            filter(lambda x: x not in [pivot_column, value_column], list(df))
        )
        df_pivot = pd.pivot_table(
            df,
            index=key_columns,
            columns=pivot_column,
            values=value_column,
            fill_value=self.config.pivot_config.fillna,
            aggfunc="sum",
        )
        return df_pivot.reset_index()
