import pandas as pd
import csv


class TableWriter(object):

    """
    The TableWriter is responsible for writing a Pandas DataFrame to a
    CSV file. It can optionally compress the output file using gzip.
    """

    def __init__(self):
        pass

    def write_csv(self, df, filepath, gzip=False):
        """
        This method writes the Pandas DataFrame to a .csv or csv.gz file.

        Args:
            df (DataFrame): the table contents
            filepath (str): output file path
            gzip (boolean): whether to gzip the output file or not

        Returns:
            None
        """

        if not type(df) is pd.DataFrame:
            raise TypeError("Wrong table type, expected pandas DataFrame")

        if not filepath:
            raise ValueError("Empty file path")

        csv_string = df.to_csv(index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC)

        if gzip:
            with gzip.open(filepath, "wt", compresslevel=5) as f:
                f.write(csv_string)
        else:
            with open(filepath, "wt", encoding="utf-8") as f:
                f.write(csv_string)
