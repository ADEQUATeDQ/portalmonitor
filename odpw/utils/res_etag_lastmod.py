# -*- coding: utf-8 -*-
import time
from odpw.analysers.quality.new.retrievability import ResRetrieveMetric
import odpw.utils.util as util
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.resource_analysers import  ResourceSize,\
    ResourceHeaderLastModifiedCountAnalyser, ResourceHeaderETagCountAnalyser,\
    ResourceHeaderFieldsCountAnalyser, ResourceHeaderExpiresCountAnalyser
from odpw.analysers.count_analysers import ResourceCount, ResourceURLValidator
from odpw.analysers.statuscodes import ResourceStatusCode
from odpw.analysers.process_period_analysers import HeadPeriod
__author__ = 'jumbrich'

from odpw.utils.util import getSnapshot,getExceptionCode,ErrorHandler as eh,\
    progressIterator


from odpw.db.models import Portal,  PortalMetaData, Dataset, Resource
from pprint import  pprint

import structlog
log =structlog.get_logger()


def headStats(dbm, sn, portalID, re_metric=False):
    total=0
    print portalID,sn
    total= dbm.countResources(snapshot=sn, portalID=portalID)
        
    log.info("Computing head lookup stats",sn=sn, count=total)
    steps= total/10 
    if steps==0:
        steps=1
    
    iter = progressIterator(Resource.iter(dbm.getResources(snapshot=sn, portalID=portalID)), total, steps)
        
    aset = AnalyserSet()
    
    rsc= aset.add(ResourceStatusCode())
    rsize= aset.add(ResourceSize())
    rc= aset.add(ResourceCount(withDistinct=True))
    ruval= aset.add(ResourceURLValidator())
    ha= aset.add(HeadPeriod())

    if re_metric:
        aset.add(ResRetrieveMetric(rsc))
    
    process_all(aset, iter)
    
    pmd = dbm.getPortalMetaData(snapshot=sn, portalID=portalID)
    aset.update(pmd)
    
    #print pmd.res_stats
    dbm.updatePortalMetaData(pmd)
    
    

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
    return "Check how many resources have   a ETag or lastmod header field"
def name():
    return 'ResEL'
def setupCLI(pa):
    pa.add_argument('-u','--url',type=str, dest='url' , help="the API url")
    

def cli(args,dbm):
    
    aset=AnalyserSet()
    rl=aset.add(ResourceHeaderLastModifiedCountAnalyser())
    re=aset.add(ResourceHeaderETagCountAnalyser())
    rex=aset.add(ResourceHeaderExpiresCountAnalyser())
    refieldcount=aset.add(ResourceHeaderFieldsCountAnalyser())
    
    iter=Resource.iter(dbm.getResourcesWithHeader())
    
    process_all(aset, iter)
    
    print 'total:', rl.getResult()[0]+rl.getResult()[1]
    print 'last-modified:',rl.getResult()[1]
    print 'etag:',re.getResult()[1]
    print 'Expires:',rex.getResult()[1]
    
    print '-------'
    for k,v in  refieldcount.getResult().items():
        print k,v
    
    
    
        