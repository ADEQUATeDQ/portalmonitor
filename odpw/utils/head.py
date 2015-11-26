from multiprocessing.process import Process
import multiprocessing
import time
import sys
from odpw.utils.head_stats import headStats
__author__ = 'jumbrich'

from odpw.db.models import Portal, Dataset, PortalMetaData, Resource

import odpw.utils.util as util
from odpw.utils.util import getSnapshot, ErrorHandler as eh, progressIndicator

from odpw.utils.timer import Timer

import structlog
log = structlog.get_logger()



from multiprocessing.dummy import Pool as ThreadPool 
from functools import partial
import urlnorm

def head (dbm, sn, seen, resource):
    try:
        props={}
        props['mime']=None
        props['size']=None
        props['redirects']=None
        props['status']=None
        props['header']=None
        with Timer(key="headLookupProcessing") as t:
            try:
                props=util.head(resource.url)
            except Exception as e:
                eh.handleError(log, "HeadLookupException", exception=e, url=resource.url, snapshot=sn,exc_info=True)
                props['status']=util.getExceptionCode(e)
                props['exception']=util.getExceptionString(e)
        
            resource.updateStats(props)
            dbm.updateResource(resource)
            
            for pid in resource.origin:
                d= seen[pid]
                if d['processed']==0:
                    ## get the pmd for this job
                    pmd = dbm.getPortalMetaData(portalID=pid, snapshot=sn)
                    if not pmd:
                        pmd = PortalMetaData(portalID=pid, snapshot=sn)
                        dbm.insertPortalMetaData(pmd)
                    pmd.headstart()
                    dbm.updatePortalMetaData(pmd)
                    d['start'] = time.time()
            
                d['processed']+=1
                if d['processed'] == d['resources']:
                    d['end'] = time.time()
                    pmd = dbm.getPortalMetaData(portalID=pid, snapshot=sn)
                    if not pmd:
                        print "AUTSCH, no pmd for ", pid
                    pmd.headend()
                    dbm.updatePortalMetaData(pmd)
                elif d['processed'] >d['resources']:
                    print ""   
                    
                seen[pid]=d 
    except Exception as e:
        eh.handleError(log, "HeadFunctionException", exception=e, url=resource.url, snapshot=sn,exc_info=True)

def getResources(dbm, snapshot, status=-1):
    resources =[]
    
    for res in dbm.getResourceWithoutHead(snapshot=snapshot, status=status, limit=10000):
        try:
            
            url=urlnorm.norm(res['url'])
            R = Resource.fromResult(dict(res))
            resources.append(R)    
        except Exception as e:
            log.debug('DropHeadLookup', exctype=type(e), excmsg=e.message, url=url, snapshot=snapshot)
    import random
    random.shuffle(resources)
    return resources
        
class HeadProcess(Process):
    def __init__(self, dbm, snapshot):
        super(HeadProcess, self).__init__()
        self.exit = multiprocessing.Event()
        
        self.dbm=dbm
        self.snapshot=snapshot
        
        self.processors=4
        
    def run(self):
        self.dbm.engine.dispose()
        with Timer(key="getFirstResourcesFromDB", verbose=True) as t:
            resources=getResources(self.dbm, self.snapshot)

        checks=0
        while not self.exit.is_set() or len(resources) != 0:

            log.info("StartHeadLookups", count=len(resources), cores=self.processors)
    
            pool = ThreadPool(processes=self.processors,) 
    
            head_star = partial(head, self.dbm, self.snapshot)
            results = pool.map(head_star, resources)
            pool.close()
            
            pool.join()

            with Timer(key="get10000ResourcesFromDB", verbose=True) as t:
                resources=getResources(self.dbm, self.snapshot)
            checks+=1
            time.sleep(60)
            if checks % 15==0:
                log.info("HeadLookupCheck", count=len(resources), cores=self.processors) 
            
    def shutdown(self):
        log.info("ShutdownInit")
        self.exit.set()
    def setProcessors(self, processors):
        self.processors=processors

def help():
    return "perform head lookups"
def name():
    return 'Head'

def setupCLI(pa):
    pa.add_argument("--force", action='store_true', help='force a full fetch, otherwise use update',dest='fetch')
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=1)
    pa.add_argument('--status', dest='status' , help="status ", type=int, default=-1)
def cli(args,dbm):
    dbm.engine.dispose()
    sn = getSnapshot(args)
    if not sn:
        return

    resources= getResources(dbm, sn, status=args.status)
    
    while len(resources) >0:
        
        pool = ThreadPool(processes=args.processors,) 
        mgr = multiprocessing.Manager()
        seen = mgr.dict()
    
        for pmd in PortalMetaData.iter(dbm.getPortalMetaDatas(snapshot=sn)):
            seen[pmd.portal_id]= {'resources':pmd.resources, 'processed':0}
    
        log.info("Starting head lookups", count=len(resources), cores=args.processors)
    
        head_star = partial(head, dbm, sn, seen)
    
        results = pool.imap_unordered(head_star, resources)
        pool.close()
    
        pool.join()
        for portalID,v in seen.items():
            if v['processed']>0:
                headStats(dbm, sn, portalID)
    
        resources= getResources(dbm, sn, status=args.status)