class ConversionError(Exception):
    @classmethod
    def from_value_error(cls, ve: ValueError):
        return cls("Cannot convert data according to json schema: " + str(ve))
