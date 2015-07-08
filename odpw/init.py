__author__ = 'jumbrich'


from db.models import Portal
from util import ErrorHandler as eh

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
    pa.add_argument('-i','--insert',action='store_true', dest='insert')

def cli(args,dbm):
    if args.plist:
        
        if args.insert or args.update:
            ok=0
            fail=0
            for l in args.plist:
                try:
                    if len(l.split(","))==2 and len(l.split(",")[1].strip())>0:
                        p = Portal.newInstance(url=l.split(",")[0].strip(), apiurl=l.split(",")[1].strip())
                        if args.insert:
                            dbm.insertPortal(p)
                        ok+=1
                    else:
                        log.info("Skipping line",line=l )
                except Exception as e:
                    eh.handleError(log, "Insert new Portal", exception=e, line=l,exc_info=True)
                    #log.error("Insert new Portal", line=l, exctype=type(e), excmsg=e.message,exc_info=True)
                    fail+=1
            log.info("Initialised portals", total=(ok+fail), ok=ok, failed=fail)