import odpw
from odpw.new.db import DBManager
from odpw.utils.timer import Timer

__author__ = 'jumbrich'

import structlog

from sqlalchemy.exc import OperationalError
import os
import yaml
import argparse
import logging.config
import time





submodules=[
            ]




def config_logging():
    
    
    
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt='iso'),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(sort_keys=True)
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
    )


def start ():
    
    start= time.time()
    pa = argparse.ArgumentParser(description='Open Portal Watch toolset.', prog='odpw')
    

    logg=pa.add_argument_group("Logging")
    logg.add_argument(
        '-d', '--debug',
        help="Print lots of debugging statements",
        action="store_const", dest="loglevel", const=logging.DEBUG,
        default=logging.WARNING
    )
    logg.add_argument(
        '-v', '--verbose',
        help="Be verbose",
        action="store_const", dest="loglevel", const=logging.INFO,
        default=logging.WARNING
    )
    
    config=pa.add_argument_group("Config")
    config.add_argument('-c','--config', help="config file", dest='config')
    
    sp = pa.add_subparsers(title='Modules', description="Available sub modules")
    for sm in submodules:
        smpa = sp.add_parser(sm.name(), help=sm.help())
        sm.setupCLI(smpa)
        smpa.set_defaults(func=sm.cli)
    
    args = pa.parse_args()
    
        
    db={
        'host':None,
        'port':None,
        'password':None,
        'user':None,
        'db':None
        }
    
    if args.config:
        print args.config
        try:
            with open(args.config) as f_conf:
                config = yaml.load(f_conf)
                if 'logging' in config:
                    print "setup logging"
                    logging.config.dictConfig(config['logging'])
                else:
                    ##load basic logging
                    logconf = os.path.join(odpw.__path__[0], 'resources', 'logging.yaml')
                    with open(logconf) as f:
                        logging.config.dictConfig(yaml.load(f))
    
                if 'db' in config:
                    for key in db:
                        if key in config['db']:
                            db[key]=config['db'][key]
        except Exception as e:
            print "Exception during config initialisation",e
            return
    else:
        ##load basic logging
        logconf = os.path.join(odpw.__path__[0], 'resources', 'logging.yaml')
        with open(logconf) as f:
            logging.config.dictConfig(yaml.load(f))
        logging.basicConfig(level=args.loglevel)

    #config the structlog
    config_logging()
    log = structlog.get_logger()
    
    try:
        log.info("CMD ARGS", args=str(args))
    
        dbm = DBManager(**db)
        args.func(args,dbm)
    except OperationalError as e:
        log.fatal("DB Connection Exception: ", msg=e.message)
        
    except Exception as e:
        log.fatal("Uncaught exception", exc_info=True)
    end = time.time()
    secs = end - start
    msecs = secs * 1000
    
    log.info("END MAIN", time_elapsed=msecs)
    

    Timer.printStats()
    
    
    


if __name__ == "__main__":
    start()
