import hashlib
import json
import string

import yaml

from odpw.core.model import Base
import structlog
log =structlog.get_logger()


def readDBConfFromFile(config):
    dbConf={ 'host':None, 'port':None, 'password':None, 'user':None, 'db':None}
    if config:
        with open(config) as f_conf:
            config = yaml.load(f_conf)
            if config is not None and 'db' in config:
                dbConf.update(config['db'])
            if config is None:
                log.error("Cannot load config file")
    #log.info("DBConfig", conf=dbConf)
    return dbConf


def md5(data):
    return hashlib.md5(json.dumps(data, sort_keys=True)).hexdigest()


def extractMimeType(ct):
    if ";" in ct:
        return str(ct)[:ct.find(";")].strip()
    return ct.strip()


def format_filename(s):
    """Take a string and return a valid filename constructed from the string.
Uses a whitelist approach: any characters not present in valid_chars are
removed. Also spaces are replaced with underscores.

Note: this method may produce invalid filenames such as ``, `.` or `..`
When I use this method I prepend a date string like '2009_01_15_19_46_32_'
and append a file extension like '.txt', so I avoid the potential of using
an invalid filename.

"""
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    filename = ''.join(c for c in s if c in valid_chars)
    filename = filename.replace(' ', '_')  # I don't like spaces in filenames.
    return filename