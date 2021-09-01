import zlib


def from_response(raw_response, gzipped=False):
    body = raw_response["Body"]

    if gzipped:
        # Plus 32 to automatically accept either the zlib or gzip format:
        # https://docs.python.org/3/library/zlib.html#zlib.decompress
        decompressor = zlib.decompressobj(wbits=32 + zlib.MAX_WBITS)
        rest = ""

        for chunk in body.iter_chunks():
            data = rest + decompressor.decompress(chunk).decode(encoding="utf-8")
            lines = data.split("\n")

            # We don't know yet whether the last element in the list is the
            # beginning of a new row or if it is the last row in the file (in
            # case the file doesn't have a final newline). Save it to be
            # prepended to the next decoded chunk (or yielded if it was the
            # last line).
            rest = lines.pop()

            for line in lines:
                yield line

        if rest:
            # `rest` will contain the last line when the file didn't have a
            # final newline.
            yield rest

    else:
        for line in body.iter_lines():
            if line:
                yield line.decode(encoding="utf-8")
