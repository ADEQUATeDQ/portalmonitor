from odpw.quality.analysers import PortalMetaDataStatusAnalyser, AnalyseEngine,\
    PortalMetaDataFetchStatsAnalyser, PortalMetaDataResourceStatsAnalyser
import time
import odpw.util as util
__author__ = 'jumbrich'

from odpw.util import getSnapshot,getExceptionCode,ErrorHandler as eh
import odpw.fetch as fetch

from odpw.db.models import Portal,  PortalMetaData, Dataset, Resource
from pprint import  pprint

import logging
log = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()


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
                if data:
                    
                    stats=fetch.extract_keys(data, stats)

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
    

def name():
    return 'FetchStats'
def setupCLI(pa):
    
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')

def cli(args,dbm):
    sn = getSnapshot(args)
    if not sn:
        return
        
    fetchStats(dbm,sn)
