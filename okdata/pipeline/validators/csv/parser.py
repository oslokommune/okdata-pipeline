class ParseErrors(Exception):
    def __init__(self, errors):
        self.errors = errors


def parse_csv(reader, schema, header=[]):
    if "items" not in schema or "properties" not in schema["items"]:
        return list(reader)

    row_schema = schema["items"]["properties"]

    errors = []
    data = []
    cleaned_header = [h.strip() for h in header]

    for row_i, row in enumerate(reader):
        insert_row = {}

        for col_i, value in enumerate(row):
            if value == "":
                continue

            key = cleaned_header[col_i] if header else f"{col_i}"
            try:
                value_type = row_schema[key]["type"]
            except KeyError:
                errors.append(
                    {
                        "row": row_i,
                        "column": key,
                        "message": f"Unexpected header: '{key}'",
                    }
                )

            try:
                insert_row[key] = parse_value(value, value_type)
            except ValueError as e:
                errors.append({"row": row_i, "column": key, "message": str(e)})

        data.append(insert_row)

    if errors:
        raise ParseErrors(errors)

    return data


def parse_value(value, value_type="string"):
    if value_type == "null":
        if value == "null":
            return None
        else:
            raise ValueError(f'Null must be "null" but was "{value}"')
    elif value_type == "integer":
        return int(value)
    elif value_type == "number":
        normalized = ".".join(value.rsplit(",", 1))
        return float(normalized)
    elif value_type == "boolean":
        if value == "true":
            return True
        elif value == "false":
            return False
        else:
            raise ValueError(f'Boolean must be "true" or "false" but was "{value}"')
    else:
        return value
