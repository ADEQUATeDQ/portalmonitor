from sys import exc_info
from sqlalchemy.exc import OperationalError
import os
import odpw

__author__ = 'jumbrich'


import argparse

import logging.config

from odpw.utils.timer import Timer

import time

from odpw.db.dbm import PostgressDBM
from odpw.utils.util import ErrorHandler as eh

import odpw.db.dbm as dbcli
import odpw.init as initcli
import odpw.utils.fetch as fetchcli
import odpw.utils.stats as statscli
import odpw.utils.datamonitor as dmcli
import odpw.utils.extractcsv as extractcli
import odpw.utils.head as headcli
import odpw.analysers.quality.quality as qualitycli
import odpw.utils.sanity as statuscli
import odpw.server.server as servercli
import odpw.utils.head_stats as headStatscli
import odpw.utils.fetch_stats as fetchStatscli

submodules=[dbcli, initcli, fetchcli,statscli, dmcli, extractcli, headcli,  statuscli, qualitycli,servercli,headStatscli,fetchStatscli]


import yaml



def start ():
    
    start= time.time()
    pa = argparse.ArgumentParser(description='Open Portal Watch toolset.', prog='odpw')


    logg=pa.add_argument_group("Logging")
    logg.add_argument(
        '-d', '--debug',
        help="Print lots of debugging statements",
        action="store_const", dest="loglevel", const=logging.DEBUG,
        default=logging.WARNING,
    )
    logg.add_argument(
        '-v', '--verbose',
        help="Be verbose",
        action="store_const", dest="loglevel", const=logging.INFO,
    )
    
    dbg=pa.add_argument_group("DB")
    dbg.add_argument('--host', help="DB host", dest='dbhost', default="localhost")
    dbg.add_argument('--port', help="DB port", dest='dbport',type=int, default=5433)
    dbg.add_argument('--user', help="DB user", dest='dbuser', default="opwu")
    dbg.add_argument('--db', help="DB database", dest='dbdb', default="portalwatch")
    dbg.add_argument('--password', help="DB password", dest='dbpwd',default='0pwu')
    
    config=pa.add_argument_group("Config")
    config.add_argument('-c','--config', help="config file", dest='config',type=file)
    
    sp = pa.add_subparsers(title='Modules', description="Available sub modules")
    for sm in submodules:
        smpa = sp.add_parser(sm.name(), help='a help')
        sm.setupCLI(smpa)
        smpa.set_defaults(func=sm.cli)

    args = pa.parse_args()
    
    ##load basic logging
    
    logconf = os.path.join(odpw.__path__[0], 'resources', 'logging.yaml')
    with open(logconf) as f:
        config = yaml.load(f)
        logging.config.dictConfig(config)
        
    db={
        'host':args.dbhost, 
        'port':args.dbport, 
        'password':args.dbpwd, 
        'user':args.dbuser, 
        'db':args.dbdb 
        }
    
    if args.config:
        config = yaml.load(args.config)
        if 'logging' in config:
            print "setup logging"
            logging.config.dictConfig(config['logging'])
        if 'db' in config:
            for key in db:
                if key in config['db']:
                    db[key]=config['db'][key]
    else:
        logging.basicConfig(level=args.loglevel,format='%(asctime)s - %(levelname)s - %(name)s:%(lineno)d  - %(message)s',datefmt="%Y-%m-%dT%H:%M:%S")

    
    #log = logging.getLogger(__name__)
    from structlog import get_logger, configure
    from structlog.stdlib import LoggerFactory
    configure(logger_factory=LoggerFactory())
    log = get_logger()
    
    try:
        log.info("CMD ARGS", args=str(args))
    
        dbm = PostgressDBM(**db)
        args.func(args,dbm)
    except OperationalError as e:
        log.fatal("DB Connection Exception: ", msg=e.message)
        
    except Exception as e:
        log.fatal("Uncaught exception", exc_info=True)
    end = time.time()
    secs = end - start
    msecs = secs * 1000
    
    log.info("END MAIN", time_elapsed=msecs)
    
    
    eh.printStats()
    
    Timer.printStats()
    
    
    


if __name__ == "__main__":
    start()
