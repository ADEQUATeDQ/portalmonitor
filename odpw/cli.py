from ConfigParser import ConfigParser
__author__ = 'jumbrich'

import argparse

import logging.config
from timer import Timer
import time

from odpw.db.dbm import PostgressDBM
from odpw.util import ErrorHandler as eh

from odpw.db import dbm as dbcli
import odpw.init as initcli
import odpw.fetch as fetchcli
import odpw.stats as statscli
import odpw.datamonitor as dmcli
import odpw.extractcsv as extractcli
import odpw.head as headcli
import odpw.quality.quality as qualitycli
import odpw.sanity as statuscli
from odpw.server import server as servercli
import odpw.head_stats as headStatscli
import odpw.fetch_stats as fetchStatscli

submodules=[dbcli, initcli, fetchcli,statscli, dmcli, extractcli, headcli,  statuscli, qualitycli,servercli,headStatscli,fetchStatscli]

def start ():
    start= time.time()
    pa = argparse.ArgumentParser(description='Open Portal Watch toolset.',prog='odpw')


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
    logg.add_argument(
        '-l', '--logconf',
        help="Load logging config",
        type=file, dest='logfile'
    )

    dbg=pa.add_argument_group("DB")
    dbg.add_argument('--host', help="DB host", dest='dbhost', default="localhost")
    dbg.add_argument('--port', help="DB port", dest='dbport',type=int, default=5433)
    dbg.add_argument('--user', help="DB user", dest='dbuser', default="opwu")
    dbg.add_argument('--db', help="DB database", dest='dbdb', default="portalwatch")
    dbg.add_argument('--password', help="DB password", dest='dbpwd',default='0pwu')
    
    config=pa.add_argument_group("Config")
    dbg.add_argument('-c','--config', help="config file", dest='config',type=file)
    
    #dbg.add_argument('--dbconf', help="DB config", dest='dbconf',type=file)

    sp = pa.add_subparsers(title='Modules', description="Available sub modules")
    for sm in submodules:
        smpa = sp.add_parser(sm.name(), help='a help')
        sm.setupCLI(smpa)
        smpa.set_defaults(func=sm.cli)

    args = pa.parse_args()

    if args.logfile:
        logging.config.fileConfig(args.logfile,disable_existing_loggers=0)
    else:
        logging.basicConfig(level=args.loglevel,format='%(asctime)s - %(levelname)s - %(name)s:%(lineno)d  - %(message)s',datefmt="%Y-%m-%dT%H:%M:%S")

    dbm= PostgressDBM(host=args.dbhost, port=args.dbport, password=args.dbpwd, user=args.dbuser, db=args.dbdb )

    print args
    args.func(args,dbm)

    end = time.time()
    secs = end - start
    msecs = secs * 1000
    logger = logging.getLogger(__name__)
    logger.info("END time elapsed %s ms",msecs)
    
    
    eh.printStats()
    
    Timer.printStats()
    
    
    


if __name__ == "__main__":
    start()
