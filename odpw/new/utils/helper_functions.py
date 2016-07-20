import hashlib
import json

import datetime
import yaml

from odpw.new.core.model import Base


def readDBConfFromFile(config):
    dbConf={ 'host':None, 'port':None, 'password':None, 'user':None, 'db':None}
    if config:
        with open(config) as f_conf:
            config = yaml.load(f_conf)
            if 'db' in config:
                dbConf.update(config['db'])
    return dbConf


def md5(data):
    return hashlib.md5(json.dumps(data, sort_keys=True)).hexdigest()


def extractMimeType(ct):
    if ";" in ct:
        return str(ct)[:ct.find(";")].strip()
    return ct.strip()

_row2dict = lambda r: {c.name: str(getattr(r, c.name)) for c in r.__table__.columns}
def row2dict(r):
    if isinstance(r, Base):
        return _row2dict(r)
    elif len(r)>1:
        print r
        d={}
        for i in range(0, len(r)):
            if r[i] is not None:
                d.update(_row2dict(r[i]))
        return d
