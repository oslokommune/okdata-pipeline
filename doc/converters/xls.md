# converters.xls

Lambda function to transform Excel files to CSV.

## Input event format

Example Lambda event input:

```json
{
    "input": "s3://bucket/key",
    "output": "s3://bucket/key",
    "config": {
        "table_has_header": true,
        "sheet_name": "Sheet1",
        "column_names": ["A", "B"],
        "table_sources": [
            {
                "start_row": 1,
                "start_col": 1
            }
        ]
    }
}
```

The config object takes the following key/values:

| Key               | Type                | Description                                       | Default value                        |
| ----------------- | ------------------- | ------------------------------------------------- | ------------------------------------ |
| table_has_header  | Boolean             | Is the first row of the table a header row?       | `true`                               |
| sheet_name        | String or Integer   | Name or index of sheet to use                     | `0` (first sheet)                    |
| column_names      | Array               | List of column names                              | `null` (use names from header row)   |
| table_sources     | Array               | List of subtable locations (start row & col)      | `[{"start_row": 1, "start_col": 1}]` |
