from dateutil.parser import isoparse


def jsonschema_datetime(value):
    try:
        isoparse(value)
        isoparse(value.replace("Z", "+00:00"))
    except Exception:
        return False
    return True


def jsonschema_year(value):
    try:
        int(value)
    except Exception:
        return False
    return True
