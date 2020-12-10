class ConversionError(Exception):
    def __init__(self, msg):
        super().__init__(f"Cannot convert data according to JSON schema: {msg}")
