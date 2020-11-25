from dateutil.parser import isoparse


def jsonschema_datetime(value):
    try:
        isoparse(value)
    except Exception:
        try:
            isoparse(value.replace("Z", "+00:00"))
        except Exception:
            return False
        return True
    return True


def jsonschema_year(value):
    try:
        int(value)
        return True
    except Exception:
        pass
    return False
