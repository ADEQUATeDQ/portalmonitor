
import time
import odpw.utils.util as util
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.resource_analysers import ResourceStatusCode, ResourceSize
__author__ = 'jumbrich'

from odpw.utils.util import getSnapshot,getExceptionCode,ErrorHandler as eh,\
    progressIterator


from odpw.db.models import Portal,  PortalMetaData, Dataset, Resource
from pprint import  pprint

import logging
log = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()


def headStats(dbm, sn, portalID):
    total=0
    print portalID,sn
    total= dbm.getResourcesCount(snapshot=sn, portalID=portalID)
        
    log.info("Computing head lookup stats",sn=sn, count=total)
    steps= total/10 
    if steps==0:
        steps=1
    
    iter = progressIterator(Resource.iter(dbm.getResources(snapshot=sn, portalID=portalID)), total, steps)
        
    pmd = dbm.getPortalMetaData(snapshot=sn, portalID=portalID)
    
    aset = AnalyserSet()
    rsc= aset.add(ResourceStatusCode())
    rsize= aset.add(ResourceSize())
    
    process_all(aset, iter)
    
    aset.update(pmd)
    
    print pmd.res_stats
    print 'res',rsc.getResult()

    

#===============================================================================
# def headStats(dbm, sn, portalID):
#     """
#     Compute the head lookup stats
#     """
#     portalStats={}
#     
#     total=0
#     
#     for res in dbm.countProcessedResources(snapshot=sn, portalID=portalID):
#         total=res[0]
#     log.info("Computing head lookup stats",sn=sn, count=total)
#     print total
#     c=0;
#     steps= total/10 
#     if steps==0:
#         steps=1
#     
#     start = time.time()
#     interim = time.time()
#     
#     if portalID:
#         portalStats[portalID]= {'res_stats':{'respCodes':{}, 'unique':0}}
#     
#     for res in dbm.getProcessedResources(snapshot=sn, portalID=portalID):
#         c+=1
#         if not res['origin']:
#             continue
#         
#         for pid in res['origin'].keys():
#             if portalID and pid != portalID:
#                 continue
#             if pid not in portalStats:
#                 portalStats[pid]= {'res_stats':{'respCodes':{}, 'unique':0}}
#             
#             stats = portalStats[pid]
#             
#             cnt= stats['res_stats']['respCodes'].get(res['status'],0)
#             stats['res_stats']['respCodes'][res['status']]= (cnt+1)
#             stats['res_stats']['unique']+=1
#             
#         if c%steps == 0:
#             elapsed = (time.time() - start)
#             interim = (time.time() - interim)
#             util.progressIndicator(c, total, elapsed=elapsed, interim=interim)
#             interim = time.time()
# 
#     
#     elapsed = (time.time() - start)
#     interim = (time.time() - interim)
#     util.progressIndicator(c, total, elapsed=elapsed, interim=interim)
#     for pid in portalStats.keys():
#         pmd = dbm.getPortalMetaData(snapshot=sn, portalID=pid)
#         
#         stats = portalStats[pid]
#         if not pmd.res_stats:
#             pmd.res_stats = {}
#             
#         for k in stats['res_stats']:
#             pmd.res_stats[k] = stats['res_stats'][k]
#         
#         
#         ##UPDATE
#         dbm.updatePortalMetaData(pmd)
#===============================================================================

def help():
    return "Compute Head stats after head lookups"
def name():
    return 'HeadStats'
def setupCLI(pa):
    
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument('-u','--url',type=str, dest='url' , help="the API url")

def cli(args,dbm):
    sn = getSnapshot(args)
    
    pids=[]
    if args.url:
        p = dbm.getPortal(apiurl=args.url)
        if p:
            pids.append(p.id)
    else:
        for p in Portal.iter(dbm.getPortals()):
            pids.append(p.id)
    snapshots=[]
    
    for pid in pids:
        if not sn:
            for s in dbm.getSnapshots(portalID=pid):
                snapshots.append(s['snapshot'])
        else:
            snapshots.append(sn)
        
        for sn in snapshots:
            headStats(dbm,sn,pid)
