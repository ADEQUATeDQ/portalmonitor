'''
Created on Aug 17, 2015

@author: jumbrich
'''
from odpw.db.dbm import PostgressDBM
from odpw.db.models import Dataset, DatasetLife, Portal
from odpw.analysers.core import DCATConverter
from odpw.analysers.fetching import DCATDatasetAge
from odpw.utils.util import getSnapshot, progressIterator, progressIndicator,\
    ErrorHandler
from _collections import defaultdict

import dateutil

from datetime import datetime, timedelta, date
from multiprocessing.pool import ThreadPool
import multiprocessing

import structlog
import time
from _functools import partial
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.datasetlife import DatasetLifeAnalyser,\
    DatasetLifeStatsAnalyser
log =structlog.get_logger()


def compute_dataset_life(dbm, job):
    try:
        Portal = job['Portal']
        sn = job['snapshot']
    
        log.info("START DatasetLifeStats Fetch", pid=Portal.id, snapshot=sn, software=Portal.software)
    
        dbm.engine.dispose()
    
    
        aset = AnalyserSet()
    
        dls=aset.add(DatasetLifeStatsAnalyser(dbm, sn, Portal))
    
        it = DatasetLife.iter(dbm.getDatasetLifeResults(portalID=Portal.id))
        total = dbm.countDatasetLifeResults(portalID=Portal.id)
    
        steps = total/10 if total>10 else 1
        process_all(aset, progressIterator(it, total, steps, label=Portal.id))
    
    
    
        pmd = dbm.getPortalMetaData(portalID=Portal.id, snapshot=sn)
        aset.update(pmd)
        dbm.updatePortalMetaData(pmd)
        
    except Exception as e:
        ErrorHandler.handleError(log, "DatasetLifeStatsException", portal=Portal.id, snapshot=sn)

def help():
    return "Update datasetlife statistics"

def name():
    return 'DatasetLifeStats'

def setupCLI(pa):
    
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=1)
    
def cli(args,dbm):
    sn = getSnapshot(args)
    
    portals=[]
    if args.url:
        p = dbm.getPortal(apiurl=args.url)
        if p:
            portals.append(p)
    else:
        for p in Portal.iter(dbm.getPortals()):
            portals.append(p)
    
    
    
    jobs=[]
    for p in portals:
        snapshots=set([])
        if not sn:
            for s in dbm.getSnapshots(portalID=p.id):
                print s['snapshot']
                snapshots.add(int(s['snapshot']))
        else:
            snapshots.add(int(sn))
        
        for sn in sorted(snapshots):
            jobs.append({'Portal':p, 'snapshot':sn})
            
    
    
    pool = ThreadPool(processes=args.processors,) 
    mgr = multiprocessing.Manager()
    seen = mgr.dict()
    
    #for pmd in PortalMetaData.iter(dbm.getPortalMetaDatas(snapshot=sn)):
    #    seen[pmd.portal_id]= {'resources':pmd.resources, 'processed':0}
    
    log.info("Starting fetch sim lookups", count=len(portals), cores=args.processors)
    
    head_star = partial(compute_dataset_life,dbm)
    
    start = time.time()
    results = pool.imap_unordered(head_star, jobs)
    pool.close()
    
    c=0
    total=len(jobs)
    steps= total/100 if total/100 !=0 else 1
    
    #for res in results:
    #    c+=1
    #    if c%steps==0:
    #        elapsed = (time.time() - start)
    #        progressIndicator(c, total, elapsed=elapsed, label="Fetch Simulate Progress")
   
    #progressIndicator(c, total, elapsed=elapsed, label="Fetch Simulate Progress")
    pool.join()
