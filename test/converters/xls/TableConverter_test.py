import os
import sys
import unittest
from copy import copy

import pandas as pd
import xlrd

from okdata.pipeline.converters.xls.TableConfig import TableConfig
from okdata.pipeline.converters.xls.TableConverter import TableConverter

CWD = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(CWD, ".."))

empty_config = TableConfig(None)

config = TableConfig(
    {
        "sheet_name": "Sheet1",
        "table_has_header": True,
        "column_names": ["A", "B"],
        "table_sources": [{"start_row": 1, "start_col": 1}],
    }
)

wrong_sheet_name_config = TableConfig(
    {
        "sheet_name": "this sheet does not exist",
        "table_has_header": True,
        "column_names": ["A", "B"],
        "table_sources": [{"start_row": 1, "start_col": 1}],
    }
)

no_header_config = TableConfig(
    {
        "sheet_name": "Sheet1",
        "column_names": ["A_specified", "B_specified"],
        "table_has_header": False,
        "table_sources": [{"start_row": 2, "start_col": 1}],
    }
)

subtables_config = TableConfig(
    {
        "sheet_name": "Sheet1",
        "table_has_header": True,
        "column_names": ["A", "B"],
        "table_sources": [
            {"start_row": 2, "start_col": 1},
            {"start_row": 2, "start_col": 4},
        ],
    }
)

invalid_subtables_config = TableConfig(
    {
        "sheet_name": "Sheet1",
        "table_has_header": True,
        "table_sources": [
            {"start_row": 2, "start_col": 1},
            {"start_row": 2, "start_col": 4},
        ],
    }
)

extra_col_config = TableConfig(
    {
        "sheet_name": "Sheet1",
        "table_has_header": True,
        "column_names": ["A", "B"],
        "extra_col": {"name": "year", "dtype": "int"},
        "table_sources": [
            {"start_row": 2, "start_col": 1, "extra_row": 1, "extra_col": 1},
            {"start_row": 2, "start_col": 4, "extra_row": 1, "extra_col": 4},
        ],
    }
)

pivot_config = TableConfig(
    {"pivot_config": {"pivot_column": "categories", "value_column": "value"}}
)


class Tester(unittest.TestCase):
    def test_read_excel_file(self):
        conv = TableConverter(config)
        wb = conv.read_excel_table(os.path.join(CWD, "data", "simple.xlsx"))

        self.assertEqual(type(wb), xlrd.book.Book)

    def test_read_wrong_sheet_name(self):
        conv = TableConverter(wrong_sheet_name_config)
        wb = conv.read_excel_table(os.path.join(CWD, "data", "simple.xlsx"))

        with self.assertRaises(OSError):
            conv.extract_sub_table(wb, wrong_sheet_name_config.table_sources[0])

    def test_read_wo_header(self):
        conv = TableConverter(no_header_config)
        wb = conv.read_excel_table(os.path.join(CWD, "data", "simple.xlsx"))
        df = conv.extract_sub_table(wb, no_header_config.table_sources[0])

        self.assertEqual(type(df), pd.DataFrame)
        self.assertEqual(list(df.columns), ["A_specified", "B_specified"])
        self.assertEqual(list(df.values[0]), [1, "foo"])
        self.assertEqual(list(df.values[2]), [3, "baz"])
        self.assertEqual(df.shape, (3, 2))

    def test_extract_sub_table(self):
        conv = TableConverter(config)
        wb = conv.read_excel_table(os.path.join(CWD, "data", "simple.xlsx"))
        df = conv.extract_sub_table(wb, config.table_sources[0])

        self.assertEqual(type(df), pd.DataFrame)
        self.assertEqual(list(df.columns), ["A", "B"])
        self.assertEqual(list(df.values[0]), [1, "foo"])
        self.assertEqual(list(df.values[2]), [3, "baz"])
        self.assertEqual(df.size, 6)

    def test_header_mismatch(self):
        bad_config = copy(config)
        bad_config.column_names = ["foo", "bar"]
        conv = TableConverter(bad_config)

        wb = conv.read_excel_table(os.path.join(CWD, "data", "simple.xlsx"))
        with self.assertRaises(ValueError):
            conv.extract_sub_table(wb, config.table_sources[0])

    def test_convert_with_empty_config(self):
        conv = TableConverter(empty_config)
        wb = conv.read_excel_table(os.path.join(CWD, "data", "simple.xlsx"))
        df = conv.convert_table(wb)

        self.assertEqual(type(df), pd.DataFrame)
        self.assertEqual(list(df.columns), ["A", "B"])
        self.assertEqual(list(df.values[0]), [1, "foo"])
        self.assertEqual(list(df.values[2]), [3, "baz"])
        self.assertEqual(df.size, 6)

    def test_multiple_subtables(self):
        conv = TableConverter(subtables_config)
        wb = conv.read_excel_table(os.path.join(CWD, "data", "subtables.xlsx"))
        df = conv.convert_table(wb)

        self.assertEqual(type(df), pd.DataFrame)
        self.assertEqual(list(df.columns), ["A", "B"])
        self.assertEqual(list(df.values[0]), [1, "foo"])
        self.assertEqual(list(df.values[2]), [3, "baz"])
        self.assertEqual(list(df.values[3]), [4, "hei"])
        self.assertEqual(list(df.values[6]), [7, "ciao"])
        self.assertEqual(df.size, 14)

    def test_invalid_subtable_config(self):
        conv = TableConverter(invalid_subtables_config)
        wb = conv.read_excel_table(os.path.join(CWD, "data", "subtables.xlsx"))
        with self.assertRaises(ValueError):
            conv.convert_table(wb)

    def test_extra_column(self):
        conv = TableConverter(extra_col_config)
        wb = conv.read_excel_table(os.path.join(CWD, "data", "subtables.xlsx"))
        df = conv.convert_table(wb)

        self.assertEqual(type(df), pd.DataFrame)
        self.assertEqual(list(df.columns), ["A", "B", "year"])
        self.assertEqual(list(df.values[0]), [1, "foo", 2016])
        self.assertEqual(list(df.values[2]), [3, "baz", 2016])
        self.assertEqual(list(df.values[3]), [4, "hei", 2017])
        self.assertEqual(list(df.values[6]), [7, "ciao", 2017])
        self.assertEqual(df.size, 21)

    def test_pivot_table(self):
        conv = TableConverter(pivot_config)
        wb = conv.read_excel_table(os.path.join(CWD, "data", "before_pivot.xlsx"))
        df = conv.convert_table(wb)

        self.assertEqual(type(df), pd.DataFrame)
        self.assertListEqual(list(df.columns), ["key_col", "men", "women"])
        self.assertListEqual(list(df["key_col"]), ["a", "b"])
        self.assertListEqual(list(df["men"]), [3, 6])
        self.assertListEqual(list(df["women"]), [5, 4])
        self.assertEqual(df.size, 6)

    def test_befolkning_pivot(self):
        config = TableConfig(
            {
                "pivot_config": {
                    "pivot_column": "Alder",
                    "value_column": "Antall personer",
                }
            }
        )
        conv = TableConverter(config)
        wb = conv.read_excel_table(
            os.path.join(CWD, "data", "Befolking_test_data.xlsx")
        )
        df = conv.convert_table(wb)
        self.assertEqual(type(df), pd.DataFrame)
        self.assertEqual(len(df.index), 2)
        self.assertEqual(list(df[99])[0], 0)
        self.assertEqual(list(df[99])[1], 3)


if __name__ == "__main__":
    unittest.main()
