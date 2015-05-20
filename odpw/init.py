__author__ = 'jumbrich'


import sys
from db.models import Portal
from db.POSTGRESManager import PostGRESManager

import logging
log = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()

def name():
    return 'Init'
def setupCLI(pa):
    pa.add_argument('-p','--portals',type=file, dest='plist')

def cli(args,dbm):
    if args.plist:
        ok=0
        fail=0
        for l in args.plist:
            try:
                if len(l.split(","))==2 and len(l.split(",")[1].strip())>0:
                    p = Portal.newInstance(url=l.split(",")[0].strip(), apiurl=l.split(",")[1].strip())
                    dbm.insertPortal(p)
                    ok+=1
                    log.info("processed")
                else:
                    log.info("Skipping line",line=l )
            except Exception as e:
                log.exception(e)
                fail+=1
        log.info("Initialised portals", total=(ok+fail), ok=ok, failed=fail)