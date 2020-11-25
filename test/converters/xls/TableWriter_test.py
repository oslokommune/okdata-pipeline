import os
import sys
import tempfile
import unittest

import pandas as pd

from okdata.pipeline.converters.xls.TableWriter import TableWriter

CWD = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(CWD, ".."))


class Tester(unittest.TestCase):
    def test_wrong_table_type(self):
        writer = TableWriter()
        with self.assertRaises(TypeError):
            writer.write_csv(0, "file.csv")

    def test_empty_filename(self):
        writer = TableWriter()
        df = pd.DataFrame()
        with self.assertRaises(ValueError):
            writer.write_csv(df, "")

    def test_write_csv(self):
        writer = TableWriter()
        df = pd.DataFrame({"A": [1, 2, 3], "B": ["foo", "bar", "baz"]})

        tempdir = tempfile.mkdtemp()
        filepath = "{dir}/output.csv".format(dir=tempdir)
        writer.write_csv(df, filepath)

        with open(filepath, "r") as f:
            csv_content = f.read()

        os.remove(filepath)
        os.rmdir(tempdir)

        expected_content = '"A";"B"\n' + '1;"foo"\n' + '2;"bar"\n' + '3;"baz"\n'

        self.assertEqual(csv_content, expected_content)


if __name__ == "__main__":
    unittest.main()
