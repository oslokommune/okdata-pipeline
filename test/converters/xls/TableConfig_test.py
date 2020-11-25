import os
import sys
import unittest

from okdata.pipeline.converters.xls.TableConfig import TableConfig

CWD = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(CWD, ".."))


class Tester(unittest.TestCase):
    def test_no_config(self):
        config = TableConfig(None)
        self.assertEqual(config.sheet_name, 0)
        self.assertEqual(config.column_names, None)
        self.assertEqual(config.table_has_header, True)
        self.assertEqual(config.table_sources[0].start_row, 1)
        self.assertEqual(config.table_sources[0].start_col, 1)

    def test_empty_config(self):
        empty_config = {}
        config = TableConfig(empty_config)
        self.assertEqual(config.sheet_name, 0)
        self.assertEqual(config.column_names, None)
        self.assertEqual(config.table_has_header, True)
        self.assertEqual(config.table_sources[0].start_row, 1)
        self.assertEqual(config.table_sources[0].start_col, 1)

    def test_malformed_config(self):
        malformed_config = []
        with self.assertRaises(TypeError):
            TableConfig(malformed_config)

    def test_column_names(self):
        config = TableConfig(
            {
                "sheet_name": "foo",
                "column_names": ["A", "B", "C"],
                "table_has_header": True,
                "table_sources": [],
            }
        )
        self.assertEqual(config.column_names, ["A", "B", "C"])

    def test_malformed_column_names(self):
        config = {
            "sheet_name": "foo",
            "column_names": [1, 2, 3],
            "table_has_header": True,
            "table_sources": [],
        }
        with self.assertRaises(TypeError):
            TableConfig(config)

    def test_missing_column_names(self):
        config = {"table_has_header": False}
        with self.assertRaises(ValueError):
            TableConfig(config)

    def test_table_sources(self):
        config = TableConfig(
            {
                "sheet_name": "foo",
                "column_names": ["A"],
                "table_has_header": True,
                "table_sources": [
                    {"start_row": 13, "start_col": 37},
                    {"start_row": 3, "start_col": 14},
                ],
            }
        )
        self.assertEqual(config.table_sources[0].start_row, 13)
        self.assertEqual(config.table_sources[0].start_col, 37)
        self.assertEqual(config.table_sources[1].start_row, 3)
        self.assertEqual(config.table_sources[1].start_col, 14)

    def test_extra_col(self):
        config = TableConfig(
            {
                "sheet_name": "foo",
                "column_names": ["A"],
                "table_has_header": True,
                "table_sources": [
                    {"start_row": 3, "start_col": 14, "extra_row": 13, "extra_col": 37}
                ],
                "extra_col": {"name": "year", "dtype": "int"},
            }
        )
        self.assertEqual(config.extra_col.name, "year")
        self.assertEqual(config.extra_col.dtype, int)
        self.assertEqual(config.table_sources[0].extra_row, 13)
        self.assertEqual(config.table_sources[0].extra_col, 37)

    def test_pivot_config(self):
        config = TableConfig(
            {"pivot_config": {"pivot_column": "p_col", "value_column": "v_col"}}
        )
        self.assertEqual(config.pivot_config.pivot_column, "p_col")
        self.assertEqual(config.pivot_config.value_column, "v_col")

    def test_wrong_pivot_config(self):
        missing_pivot_column = {"pivot_config": {"value_column": "v_col"}}
        missing_value_column = {"pivot_config": {"pivot_column": "p_col"}}
        with self.assertRaises(ValueError):
            TableConfig(missing_pivot_column)
        with self.assertRaises(ValueError):
            TableConfig(missing_value_column)


if __name__ == "__main__":
    unittest.main()
