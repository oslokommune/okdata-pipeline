# validators.csv

Pipeline component for validating CSV against a JSON schema.

First the function will try to parse the CSV using the JSON schema by trying to
convert string values to numbers/booleans/etc. If this is successful, the
resulting data structure will be validated against the JSON schema.

## Input

Input/output S3 buckets and a JSON schema must be supplied.

Example input object:

```json
{
  "input": "s3://bucket/key",
  "schema": "<inline JSON schema string>"
}
```

The input event object takes the following key/values:

| Key               | Type      | Description                                       | Default value                     |
| ----------------- | --------- | ------------------------------------------------- | --------------------------------- |
| input             | String    | The S3 input path                                 | <no default, must be supplied>    |
| schema            | String    | Inline JSON schema                                | <no default, must be supplied>    |
| header_row        | Boolean   | Is the first row of the input file a header row?  | `true`                            |
| delimiter         | String    | The CSV delimiter used, e.g. ',' or ';'           | `;`                               |
| quote             | String    | Quote marks used, e.g. '"'                        | `"`                               |


## Output

The function returns a list of error objects and a boolean value that indicates
if the validation was successful.

```
{
    "isValid": false,
    "errors": [
        {
            "row": 0,
            "column": "foo",
            "message": "blabla",
        }
    ]
}
```

 - "row" is the row index, excluding any header if present. Ie. in a csv
   document with header on the first line, an error on "row" 0 would be an error
   on line 1 in the csv document.
 - "column" is the name of the column if headers are present, or a column index
   if there are no headers.
 - "message" is the error message.

If parsing is not successful schema validation will _not_ take place, so the
resulting list of errors will not be complete if there are both parsing and
schema validation errors.

Example output with no errors:

```
{
    "isValid": true,
    "errors": []
}
```

## JSON schema

Read [Understanding JSON
Schema](https://json-schema.org/understanding-json-schema/index.html) to learn
more about JSON schema.

The gist for CSV validation is to define the root element as an array that
contains objects. The object properties are either the header names if a header
is present or the column index (0 based) if no header.

### Example with header

```
id;title;price
1;Foo;999.99
2;Bar;842.01
```

```
{
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "id": {
                "type": "integer",
            },
            "title": {
                "type": "string"
            },
            "price": {
                "type": "number",
            }
        }
    }
}
```

### Example without header

```
1;Foo;999.99
2;Bar;842.01
```

```
{
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "0": {
                "type": "integer",
            },
            "1": {
                "type": "string"
            },
            "2": {
                "type": "number",
            }
        }
    }
}
```
