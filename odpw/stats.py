__author__ = 'jumbrich'


from util import getSnapshot
import util

from db.models import Portal
from db.models import PortalMetaData
from urlparse import  urlparse
from collections import defaultdict

import logging
log = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()

def portalOverview(dbm):
    
    statusMap=util.initStatusMap()
    size={'datasets':0, 'resources':0}
    tldDist=defaultdict(int)
    countryDist=defaultdict(int)
    for pRes in dbm.getPortals():
        p = Portal.fromResult(pRes)
        
        util.analyseStatus(statusMap, p.status)
        
        if p.datasets !=-1:
            size['datasets']+=p.datasets
        if p.resources != -1:
            size['resources']+=p.resources

        url_elements = urlparse(p.url).netloc.split(".")
        tld = ".".join(url_elements[-1:])
        tldDist[tld]+=1
        countryDist[p.country]+=1

    portalStats={"resp_codes":statusMap, "size":size, 'tlds':dict(tldDist), 'countries':dict(countryDist)}
    return portalStats

def computePortalStats(dbm,sn):
    portal_stats={
        'count':0,
        'datasets':0,
        'datasetDist':defaultdict(int),
        'resources':0,
        'resourceDist':defaultdict(int),
        'respCodes':util.initStatusMap(),
        'softwareDist':defaultdict(int),
        'process-stats':{'fetched':0, 'res':0, 'qa':0}
    }
    for pmdRes in dbm.getPortalMetaData(snapshot=sn):
        pmd=PortalMetaData.fromResult(pmdRes)
        
        #count number of portals
        portal_stats['count']+=1
        
        p = dbm.getPortal(id=pmd.portal)
        
        if p:
            portal_stats['softwareDist'][p.__dict__['software']]+=1
        
        if len(pmd.fetch_stats) >0 and 'datasets' in pmd.fetch_stats:
            portal_stats['process-stats']['fetched']+=1
            
            portal_stats['datasets']+=pmd.fetch_stats['datasets']
            
            portal_stats['datasetDist'][pmd.fetch_stats['datasets']] += 1
            
            if 'portal_status' in pmd.fetch_stats:
                util.analyseStatus(portal_stats['respCodes'], pmd.fetch_stats['portal_status'])
            elif 'respCodes' not in pmd.fetch_stats:
                util.analyseStatus(portal_stats['respCodes'], 200)
            elif 'respCodes' in pmd.fetch_stats and len(pmd.fetch_stats['respCodes'])>0:
                util.analyseStatus(portal_stats['respCodes'], 200)
            else:
                util.analyseStatus(portal_stats['respCodes'], 800)
                
        if len(pmd.res_stats) >0:
            portal_stats['process-stats']['res']+=1
            portal_stats['resources']+=pmd.res_stats['total']
            
            portal_stats['resourceDist'][pmd.res_stats['total']]+=1
            
        if len(pmd.qa_stats) >0:
            portal_stats['process-stats']['qa']+=1
    
    
    
    return portal_stats


def computeResourceStats(dbm,sn):
    stats={
            'content-length':0,
            'mime-dist':{},
            'respCodes':util.initStatusMap()
        }
    res = dbm.selectQuery("SELECT * FROM resources WHERE snapshot='"+sn+"'")

    for resJson in res:
        util.analyseStatus(stats['respCodes'], resJson['status'])
        if resJson['size']>0:
            stats['content-length']+=resJson['size']
        if resJson['mime']:
            c = stats['mime-dist'].get(resJson['mime'],0)
            stats['mime-dist'][resJson['mime']]=(c+1)
    
    return stats        
    
def computeDatasetStats(dbm,sn):
    stats={
            'respCodes':util.initStatusMap()
        }    

    return stats   

def snapshotStats(dbm, sn):
    
    stats={
        'portal_stats':computePortalStats(dbm,sn),
        'dataset_stats':computeDatasetStats(dbm,sn),
        'resource_stats':computeResourceStats(dbm,sn),
        'qa_stats':{}
        }
    
    return stats

def systemStats(dbm,sn):
    log.info("Computing fetch stats",sn=sn)

    ###
    #Portal Overview
    ###
    portalStats= portalOverview(dbm)
    print portalStats
    
    ###
    # single snapshot stats
    ###
    status=snapshotStats(dbm, sn)
    
    
    dbm.upsertSnapshotStats(status, sn)
    
    import pprint
    pprint.pprint(status)
    print util.convertSize(status['resource_stats']['content-length'])
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


