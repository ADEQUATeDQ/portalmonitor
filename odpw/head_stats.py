from odpw.quality.analysers import PortalMetaDataStatusAnalyser, AnalyseEngine,\
    PortalMetaDataFetchStatsAnalyser, PortalMetaDataResourceStatsAnalyser
import time
import odpw.util as util
__author__ = 'jumbrich'

from odpw.util import getSnapshot,getExceptionCode,ErrorHandler as eh


from odpw.db.models import Portal,  PortalMetaData, Dataset, Resource
from pprint import  pprint

import logging
log = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()


def headStats(dbm, sn):
    """
    Compute the head lookup stats
    """
    portalStats={}
    
    total=0
    
    for res in dbm.countProcessedResources(snapshot=sn):
        total=res[0]
    log.info("Computing head lookup stats",sn=sn, count=total)
    print total
    c=0;
    steps=total/10
    
    start = time.time()
    interim = time.time()
    for res in dbm.getProcessedResources(snapshot=sn):
        c+=1
        if not res['origin']:
            continue
        
        
        for portalID in res['origin'].keys():
            if portalID not in portalStats:
                portalStats[portalID]= {'res_stats':{'respCodes':{},'total':0, 'resList':[]}}
            stats = portalStats[portalID]
        cnt= stats['res_stats']['respCodes'].get(res['status'],0)
        stats['res_stats']['respCodes'][res['status']]= (cnt+1)
        stats['res_stats']['total']+=1
        
        if res['url'] not in stats['res_stats']['resList']:
            stats['res_stats']['resList'].append(res['url'])
        
        if c%steps == 0:
            elapsed = (time.time() - start)
            interim = (time.time() - interim)
            util.progressINdicator(c, total, elapsed=elapsed, interim=interim)
            interim = time.time()

    interim = (time.time() - interim)
    util.progressINdicator(c, total, elapsed=elapsed, interim=interim)
    for portalID in portalStats.keys():
        pmd = dbm.getPortalMetaData(snapshot=sn, portalID=portalID)
        
        pmd.updateStats(portalStats[portalID])
        ##UPDATE
        dbm.updatePortalMetaData(pmd)


def name():
    return 'HeadStats'
def setupCLI(pa):
    
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')

def cli(args,dbm):
    sn = getSnapshot(args)
    if not sn:
        return
        
    headStats(dbm,sn)
