class TableConfig(object):

    """
    The TableConfig contains the information needed to extract data
    from an Excel file to build a table.
    This information may be taken from several source sub-tables.
    """

    def __init__(self, json_config):
        """
        Initiates a TableConfig object.

        Args:
            json_config (dict): The table configuration as a JSON object
        """
        self.sheet_name = 0  # First sheet
        self.column_names = None
        self.table_has_header = True
        self.table_sources = [TableSource({"start_row": 1, "start_col": 1})]
        self.extra_col = None
        self.pivot_config = None

        if json_config is not None:
            if not type(json_config) is dict:
                raise TypeError("Table configuration must be a JSON object")

            self.sheet_name = json_config.get("sheet_name", 0)
            self.column_names = json_config.get("column_names", None)
            self.table_has_header = json_config.get("table_has_header", True)

            if "table_sources" in json_config:
                self.table_sources = [
                    TableSource(s) for s in json_config["table_sources"]
                ]

            if self.column_names:
                for n in self.column_names:
                    if not type(n) is str:
                        raise TypeError("Column names must be strings")

            if not self.table_has_header and not self.column_names:
                raise ValueError(
                    "Must specify column names if table does not have header row"
                )

            if "extra_col" in json_config:
                self.extra_col = ExtraCol(json_config["extra_col"])

            if "pivot_config" in json_config:
                self.pivot_config = PivotConfig(json_config["pivot_config"])


class TableSource(object):

    """
    The TableSource represents the location of a single source sub-table
    in the Excel file.

    It defines the starting row and column of the sub-table. In addition,
    it may contain the location of the cell that will populate the
    extra column.
    """

    def __init__(self, json_source):
        """
        Initiates a TableSource object.

        Args:
            json_source (dict): The table source configuration as a JSON object.
        """
        if not type(json_source) is dict:
            raise TypeError("Table source must be a JSON object")

        if not json_source:
            raise ValueError("Empty table source")

        if "start_row" not in json_source:
            raise ValueError('Table source is missing required field "start_row"')

        if "start_col" not in json_source:
            raise ValueError('Table source is missing required field "start_col"')

        if not type(json_source["start_row"]) is int:
            raise TypeError("Table source start row must be an integer")

        if not type(json_source["start_col"]) is int:
            raise TypeError("Table source start column must be an integer")

        self.start_row = json_source["start_row"]
        self.start_col = json_source["start_col"]
        self.extra_row = None
        self.extra_col = None

        if "extra_row" in json_source:
            self.extra_row = json_source["extra_row"]

        if "extra_col" in json_source:
            self.extra_col = json_source["extra_col"]


class ExtraCol(object):

    """
    Represents the configuration of the optional extra column;
    name and data type.
    """

    def __init__(self, json_extra_col):
        """
        Initiates an ExtraCol object.

        Args:
            json_extra_col (dict): The extra column configuration as a JSON object.
        """
        if not type(json_extra_col) is dict:
            raise TypeError("Table extra column config must be a JSON object")

        if not json_extra_col:
            raise ValueError("Empty table extra column configuration")

        if "name" not in json_extra_col:
            raise ValueError(
                'Table extra column config is missing required field "name"'
            )

        if "dtype" not in json_extra_col:
            raise ValueError(
                'Table extra column config is missing required field "dtype"'
            )

        if not type(json_extra_col["name"]) is str:
            raise TypeError("Table extra column name must be a string")

        self.name = json_extra_col["name"]
        self.dtype = self.to_type(json_extra_col["dtype"])

    def to_type(self, dtype):
        if dtype == "str":
            return str
        elif dtype == "int":
            return int
        elif dtype == "float":
            return float
        else:
            raise ValueError("Table extra column type must be str, int or float")


class PivotConfig(object):
    """
    Represents the optional configuration for pivot operations on the input table.
    """

    def __init__(self, pivot_config):
        if "pivot_column" not in pivot_config:
            raise ValueError(
                'Table pivot config is missing required field "pivot_column"'
            )
        if "value_column" not in pivot_config:
            raise ValueError(
                'Table pivot config is missing required field "value_column"'
            )

        self.pivot_column = pivot_config["pivot_column"]
        self.value_column = pivot_config["value_column"]
        self.fillna = pivot_config["fillna"] if "fillna" in pivot_config else 0
