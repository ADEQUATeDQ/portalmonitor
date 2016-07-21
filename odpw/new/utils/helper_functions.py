import hashlib
import json

import yaml

from odpw.new.core.model import Base
import structlog
log =structlog.get_logger()


def readDBConfFromFile(config):
    dbConf={ 'host':None, 'port':None, 'password':None, 'user':None, 'db':None}
    if config:
        with open(config) as f_conf:
            config = yaml.load(f_conf)
            if 'db' in config:
                dbConf.update(config['db'])
    log.info("DBConfig", conf=dbConf)
    return dbConf


def md5(data):
    return hashlib.md5(json.dumps(data, sort_keys=True)).hexdigest()


def extractMimeType(ct):
    if ";" in ct:
        return str(ct)[:ct.find(";")].strip()
    return ct.strip()


