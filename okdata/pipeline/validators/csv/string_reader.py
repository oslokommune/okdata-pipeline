import gzip


def from_response(raw_response, gzipped=False):
    lines = raw_response["Body"].iter_lines()

    if gzipped:
        for line in gzip.decompress(next(lines)).decode(encoding="utf-8").split("\n"):
            if line:
                yield line

    for line in lines:
        yield line.decode(encoding="utf-8")
