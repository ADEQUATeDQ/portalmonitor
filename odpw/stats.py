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

def portalOverview(dbm):
    statusMap=util.initStatusMap()
    size={'datasets':0, 'resources':0}
    for pRes in dbm.getPortals():
        p = Portal.fromResult(pRes)
        util.analyseStatus(statusMap, p.status)
        if p.datasets !=-1:
            size['datasets']+=p.datasets
        if p.resources != -1:
            size['resources']+=p.resources

    return statusMap, size

def snapshotStats(dbm, sn):
    sn_status={
        'datasets':0,
        'resources':0,
        'content-length':0,
        'mime-dist':{},
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
        if len(pmd.qa_stats) >0:
            status['qa']+=1

    resstatusMap=util.initStatusMap()
    res = dbm.selectQuery("SELECT * FROM resources WHERE snapshot='"+sn+"'")

    for resJson in res:
        util.analyseStatus(resstatusMap, resJson['status'])
        if resJson['size']>0:
            sn_status['content-length']+=resJson['size']
        if resJson['mime']:
            c = sn_status['mime-dist'].get(resJson['mime'],0)
            sn_status['mime-dist'][resJson['mime']]=(c+1)
    sn_status['res_status']=resstatusMap


    return sn_status

def systemStats(dbm,sn):
    log.info("Computing fetch stats",sn=sn)

    ###
    #Portal Overview
    ###
    statusMap, size= portalOverview(dbm)
    print statusMap
    print size

    ###
    # single snapshot stats
    ###
    sn_status=snapshotStats(dbm, sn)
    import pprint
    pprint.pprint(sn_status)
    print util.convertSize(sn_status['content-length'])
    ###
    #
    ###






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


