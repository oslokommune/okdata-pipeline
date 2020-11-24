def from_response(raw_response):
    for line in raw_response["Body"].iter_lines():
        yield line.decode(encoding="utf-8")
