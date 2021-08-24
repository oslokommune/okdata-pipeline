import gzip


def from_response(raw_response, gzipped=False):
    lines = raw_response["Body"].iter_lines()

    if gzipped:
        compressed_line = None

        try:
            compressed_line = next(lines)
        except StopIteration:
            yield ""

        if compressed_line:
            for line in (
                gzip.decompress(compressed_line).decode(encoding="utf-8").split("\n")
            ):
                if line:
                    yield line

    else:
        for line in lines:
            yield line.decode(encoding="utf-8")
