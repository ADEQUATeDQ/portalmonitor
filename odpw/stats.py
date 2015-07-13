__author__ = 'jumbrich'

from util import getSnapshot,getExceptionCode,ErrorHandler as eh

import util
import sys
import time
from timer import Timer

import math
from db.models import Portal,  PortalMetaData, Dataset, Resource
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
    for pmdRes in dbm.getPortalMetaDatas(snapshot=sn):
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
def extract_keys(data, stats):

    core=stats['general_stats']['keys']['core']
    extra=stats['general_stats']['keys']['extra']
    res=stats['general_stats']['keys']['res']

    for key in data.keys():
        if key == 'resources':
            for r in data['resources']:
                for k in r.keys():
                    if k not in res:
                        res.append(k)
        elif key == 'extras' and isinstance(data['extras'],dict):
            for k in data['extras'].keys():
                if k not in extra:
                    extra.append(k)
        else:
            if key not in core:
                core.append(key)

    return stats

def simulateFetch(portal, dbm, snapshot):
    log.info("Simulate Fetching", pid=portal.id, sn=snapshot)

    stats={
        'portal':portal,
        'datasets':-1, 'resources':-1,
        'status':-1,
        'fetch_stats':{'respCodes':{}, 'fullfetch':True},
        'general_stats':{
            'datasets':0,
            'keys':{'core':[],'extra':[],'res':[]}
        },
        'res_stats':{'respCodes':{},'total':0, 'resList':[]}
    }
    
    pmd = dbm.getPortalMetaData(portalID=portal.id, snapshot=snapshot)
    if not pmd:
        pmd = PortalMetaData(portal=portal.id, snapshot=snapshot)
        dbm.insertPortalMetaData(pmd)
    else:
        pmd.fetchstart()
        dbm.updatePortalMetaData(pmd)
    
    try:
        stats['res']=[]
        
        total=0
        for res in dbm.countDatasets(portalID=portal.id, snapshot=snapshot):
            total=res[0]
        
        print "Analysing ", total, "datasets"
        c=0
        steps=total/10
        if steps ==0:
            steps=1
        for ds in dbm.getDatasets(portalID=portal.id, snapshot=snapshot):
            c+=1
            stats['status']=200
            dataset = Dataset.fromResult(dict(ds))
            try:
                cnt= stats['fetch_stats']['respCodes'].get(dataset.status,0)
                stats['fetch_stats']['respCodes'][dataset.status]= (cnt+1)

                data =dataset.data
                
                stats=extract_keys(data, stats)

                if 'resources' in data:
                    stats['res'].append(len(data['resources']))
                    for resJson in data['resources']:
                        stats['res_stats']['total']+=1
                        
                        tR =  Resource.newInstance(url=resJson['url'], snapshot=snapshot)
                        R = dbm.getResource(tR)
                        if not R:
                            #do the lookup
                            R = Resource.newInstance(url=resJson['url'], snapshot=snapshot)
                            try:
                                dbm.insertResource(R)
                            except Exception as e:
                                print e, resJson['url'],'-',snapshot

                        R.updateOrigin(pid=portal.id, did=dataset.dataset)
                        dbm.updateResource(R)

                dbm.updateDataset(dataset)
            except Exception as e:
                eh.handleError(log,"fetching dataset information", exception=e, apiurl=portal.apiurl,exc_info=True, dataset=dataset.dataset)
            if c%steps == 0:
                util.progressINdicator(c, total)
            
            stats['resources']=sum(stats['res'])
        stats['datasets']=c
    except Exception as e:
        eh.handleError(log,"fetching dataset information", exception=e, apiurl=portal.apiurl,exc_info=True)
    try:
        pmd.updateStats(stats)
        ##UPDATE
        #General portal information (ds, res)
        #Portal Meta Data
        #   ds-fetch statistics
        dbm.updatePortalMetaData(pmd)
    except Exception as e:
        eh.handleError(log,'Updating DB',exception=e,pid=portal.id, exc_info=True)
        log.critical('Updating DB', pid=portal.id, exctype=type(e), excmsg=e.message,exc_info=True)

    

def fetchStats(dbm, sn):
    """
    Compute the fetchStats lookup stats
    """
    
    for p in dbm.getPortals():
        portal = Portal.fromResult(dict(p))
        
        simulateFetch(portal, dbm,sn)
    
    
    
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
    return 'Stats'
def setupCLI(pa):
    pa.add_argument('-s','--system',  action='store_true', dest='system', help='compute system statistics')
    
    pa.add_argument('--head',  action='store_true', dest='head', help='compute head lookup statistics')
    pa.add_argument('--fetch',  action='store_true', dest='fetch', help='recompute fetch statistics')
    
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')

def cli(args,dbm):
    sn = getSnapshot(args)
    if not sn:
        return
        
    if args.system:
        systemStats(dbm,sn)
    if args.head:
        headStats(dbm, sn)
    if args.fetch:
        fetchStats(dbm, sn)
        
    Timer.printStats()
    log.info("Timer", stats=Timer.getStats())
    
    eh.printStats()

