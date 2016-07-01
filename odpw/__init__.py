import os
import yaml
import logging
import logging.config
import odpw
from cli import config_logging

__author__ = 'jumbrich'

#import urllib3.contrib.pyopenssl
#urllib3.contrib.pyopenssl.inject_into_urllib3()

#import urllib3
#urllib3.disable_warnings()

logconf = os.path.join(odpw.__path__[0], 'resources', 'logging.yaml')
with open(logconf) as f:
    logging.config.dictConfig(yaml.load(f))
config_logging()