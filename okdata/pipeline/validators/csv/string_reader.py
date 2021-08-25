import gzip


def from_response(raw_response, gzipped=False):
    body = raw_response["Body"]

    if gzipped:
        data = b""

        for chunk in body.iter_chunks():
            data += chunk

        for line in gzip.decompress(data).decode(encoding="utf-8").split("\n"):
            if line:
                yield line

    else:
        for line in body.iter_lines():
            if line:
                yield line.decode(encoding="utf-8")
