__author__ = 'jumbrich'

import argparse
import logging
import logging.config

from db import POSTGRESManager as dbcli
from db.dbm import PostgressDBM
from db.POSTGRESManager import PostGRESManager
from util import ErrorHandler as eh
from timer import Timer

import init as initcli
import fetch as fetchcli
import stats as statscli
import time
import datamonitor as dmcli
import extractcsv as extractcli
import head as headcli

submodules=[dbcli, initcli, fetchcli,statscli, dmcli, extractcli, headcli]

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


    dbm= PostgressDBM(host=args.dbhost, port=args.dbport)



    args.func(args,dbm)

    end = time.time()
    secs = end - start
    msecs = secs * 1000
    logger = logging.getLogger(__name__)
    logger.info("END time elapsed %s ms",msecs)
    
    eh.printStats()
    for exc, count in eh.exceptions.iteritems():
        print exc, count
        
    Timer.printStats()
    
    
    


if __name__ == "__main__":
    start()
