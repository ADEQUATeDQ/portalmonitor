__author__ = 'jumbrich'

import util
from util import getSnapshot
from util import analyseStatus

from db.models import Portal
from db.models import PortalMetaData


import logging
log = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()


def fetchStats(dbm,sn):
    log.info("Computing fetch stats",sn=sn)

    #get all portals
    portals=util.initStatusMap()

    for pRes in dbm.getPortals():
        p = Portal.fromResult(pRes)

        analyseStatus(portals, p.status)

    log.info("portals status", total=portals['count'], active=portals['active'], offline=portals['offline'], servererror=portals['servererr'], connectionerror=portals['connerr'])

    status={'fetched':0, 'res':0, 'qa':0, 'count':0}
    for pmdRes in dbm.getPortalMetaData(snapshot=sn):
        status['count']+=1
        pmd=PortalMetaData.fromResult(pmdRes)
        if len(pmd.fetch_stats) >0:
            status['fetched']+=1
        if len(pmd.res_stats) >0:
            status['res']+=1
        if len(pmd.qa_stats) >0:
            status['qa']+=1
    log.info("system status", total=status['count'], fetched=status['fetched'], qa=status['qa'], res=status['res'])




def name():
    return 'Stats'
def setupCLI(pa):
    pa.add_argument('-f','--fetch',  action='store_true', dest='fetch', help='compute fetch statistics')
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')

def cli(args,dbm):


    if args.fetch:
        sn = getSnapshot(args)
        if not sn:
            return
        fetchStats(dbm,sn)


