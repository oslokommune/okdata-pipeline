class DistributionNotCreated(Exception):
    pass


class IncompleteTransaction(Exception):
    pass


class RollbackFailed(Exception):
    pass


class IllegalWrite(Exception):
    def __init__(self, message, *args):
        super().__init__("illegal write operation: ", message, *args)
