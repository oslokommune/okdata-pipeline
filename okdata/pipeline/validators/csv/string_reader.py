import zlib


def from_response(raw_response, gzipped=False):
    body = raw_response["Body"]

    if gzipped:
        # Plus 32 to automatically accept either the zlib or gzip format:
        # https://docs.python.org/3/library/zlib.html#zlib.decompress
        decompressor = zlib.decompressobj(wbits=32 + zlib.MAX_WBITS)
        rest = b""

        for chunk in body.iter_chunks():
            data = rest + decompressor.decompress(chunk)
            lines = data.split(b"\n")

            # Save the last "line" to be prepended to the next chunk (or
            # yielded if it was the last line) to cover two cases:
            #
            # 1. We don't know yet whether the last element in the list is the
            #    beginning of a new row or if it is the last row in the file
            #    (in case the file doesn't have a final newline).
            #
            # 2. Since UTF-8 code points can be multi-byte, we risk splitting
            #    the stream in the middle of a code point, making the last part
            #    incomplete and undecodable.
            rest = lines.pop()

            for line in lines:
                yield line.decode("utf-8")

        if rest:
            # `rest` will contain the last line when the file didn't have a
            # final newline.
            yield rest.decode("utf-8")

    else:
        for line in body.iter_lines():
            if line:
                yield line.decode("utf-8")
