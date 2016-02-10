import sys


__author__ = 'jumbrich'

from sys import exc_info

import structlog


from sqlalchemy.exc import OperationalError
import os
import odpw
import argparse

import logging.config

from odpw.utils.timer import Timer

import time

from odpw.db.dbm import PostgressDBM
from odpw.utils.util import ErrorHandler as eh

import odpw.db.dbm as dbcli
import odpw.init as initcli
import odpw.utils.fetch as fetchcli
#import odpw.utils.stats as statscli
import odpw.utils.datamonitor as dmcli
#import odpw.utils.extractcsv as extractcli
import odpw.utils.head as headcli
#import odpw.analysers.quality.quality as qualitycli
import odpw.utils.sanity as statuscli
import odpw.server.server as servercli
import odpw.utils.head_stats as headStatscli
import odpw.utils.fetch_stats as fetchStatscli
import odpw.utils.report as reportcli
import odpw.utils.migrate as migratecli
import odpw.utils.datset_life as datasetlifecli
import odpw.utils.datset_life_stats as datasetlifestatscli
import odpw.utils.quality as qualitycli
import odpw.server.rest.server as restcli

import odpw.utils.url_extraction as urlscli
import odpw.utils.datamonitoruris as dmuriscli
import odpw.utils.accuracy as accuracycli
import freshness.portal_changes as changescli
import odpw.utils.res_etag_lastmod as rescli
import odpw.utils.dataset_change as dschangecli

import odpw.utils.ckanfetch as ckanfetchcli
submodules=[dbcli, initcli, fetchcli,  
            statuscli, servercli,
            reportcli, dmcli, 
            headcli, migratecli,
            headStatscli,fetchStatscli,
            datasetlifecli,datasetlifestatscli,
            qualitycli,
            ckanfetchcli,
            restcli,
            urlscli,
            dmuriscli,
            accuracycli,
            changescli,
            rescli,
            dschangecli
            ]


import yaml



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
    config.add_argument('-c','--config', help="config file", dest='config')
    
    sp = pa.add_subparsers(title='Modules', description="Available sub modules")
    for sm in submodules:
        smpa = sp.add_parser(sm.name(), help=sm.help())
        sm.setupCLI(smpa)
        smpa.set_defaults(func=sm.cli)
    
    args = pa.parse_args()
    
        
    db={
        'host':args.dbhost, 
        'port':args.dbport, 
        'password':args.dbpwd, 
        'user':args.dbuser, 
        'db':args.dbdb 
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
