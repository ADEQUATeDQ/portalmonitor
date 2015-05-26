__author__ = 'jumbrich'

import util
from util import getSnapshot
import util

from db.models import Portal
from db.models import PortalMetaData


import logging
log = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()


def systemStats(dbm,sn):
    log.info("Computing fetch stats",sn=sn)

    #get all portals
    statusMap=util.initStatusMap()
    size={'datasets':0, 'resources':0}


    for pRes in dbm.getPortals():
        p = Portal.fromResult(pRes)
        util.analyseStatus(statusMap, p.status)
        size['datasets']+=p.datasets
        size['resources']+=p.resources

    print statusMap
    print size

    sn_status={
        'datasets':0,
        'resources':0,
        'process-stats':{'fetched':0, 'res':0, 'qa':0, 'count':0}
    }

    status=sn_status['process-stats']
    for pmdRes in dbm.getPortalMetaData(snapshot=sn):
        status['count']+=1
        pmd=PortalMetaData.fromResult(pmdRes)
        if len(pmd.fetch_stats) >0:
            status['fetched']+=1
            sn_status['datasets']+=pmd.fetch_stats['datasets']
        if len(pmd.res_stats) >0:
            status['res']+=1
            sn_status['resources']+=pmd.res_stats['total']
            print pmd.res_stats
        if len(pmd.qa_stats) >0:
            status['qa']+=1

    res = dbm.selectQuery("SELECT ")
    print sn_status


def name():
    return 'Stats'
def setupCLI(pa):
    pa.add_argument('-s','--system',  action='store_true', dest='system', help='compute system statistics')
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')

def cli(args,dbm):


    if args.system:
        sn = getSnapshot(args)
        if not sn:
            return
        systemStats(dbm,sn)


