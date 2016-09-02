#Normalise format
import datetime


def normaliseFormat(v):
    if v is None:
        return None
    v = v.encode('utf-8').strip()
    v = v.lower()
    if v.startswith('.'):
        v = v[1:]
    return v


def toDatetime(value):
    date=None
    if value:
        for pattern in [ "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
            try:
                date= datetime.datetime.strptime(value.split(".")[0][:19], pattern)
                break
            except Exception as e:
                pass
    return date