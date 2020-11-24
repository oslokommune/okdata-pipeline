class ConversionError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

    @classmethod
    def from_value_error(cls, ve: ValueError):
        return cls("Cannot convert data according to json schema: " + str(ve))
