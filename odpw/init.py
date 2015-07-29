__author__ = 'jumbrich'


from odpw.db.models import Portal
from odpw.utils.util import ErrorHandler as eh

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
    pa.add_argument('-u','--update',action='store_true', dest='update')
    pa.add_argument('-s','--software', dest='software', default='CKAN')

def cli(args,dbm):
    if args.plist:
        if args.insert or args.update:
            ok=0
            fail=0
            for l in args.plist:
                try:
                    if args.software == 'CKAN':
                        if len(l.split(","))==2 and len(l.split(",")[1].strip())>0:
                            url = l.split(",")[0].strip()
                            apiurl=l.split(",")[1].strip()
                        else:

                            log.info("Skipping line",line=l )
                            continue
                    else:
                        url = l.split(",")[0].strip()
                        apiurl=url


                    if args.insert:
                        p = dbm.getPortal(url=url, apiurl=apiurl)
                        if p:
                            print "Portal", p.url, "exists"
                        else:
                            p = Portal.newInstance(url=url, apiurl=apiurl,software=args.software)
                            dbm.insertPortal(p)
                            ok+=1
                    if args.update:
                        p = dbm.getPortal(url=l.split(",")[0].strip())
                        pn= Portal.newInstance(url=url, apiurl=apiurl,software=args.software)
                        if p:
                            p.apiurl=pn.apiurl
                            p.exception = pn.exception
                            dbm.updatePortal(p)
                        else:
                            dbm.insertPortal(pn)


                except Exception as e:
                    eh.handleError(log, "Insert new Portal", exception=e, line=l,exc_info=True)
                    #log.error("Insert new Portal", line=l, exctype=type(e), excmsg=e.message,exc_info=True)
                    fail+=1
            log.info("InitPortals DONE", total=(ok+fail), ok=ok, failed=fail)