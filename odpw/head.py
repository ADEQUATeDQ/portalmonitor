from multiprocessing.process import Process
import multiprocessing
from time import sleep
__author__ = 'jumbrich'

from odpw.db.models import Portal, Dataset, PortalMetaData, Resource

import odpw.util as util
from odpw.util import getSnapshot,getExceptionCode,ErrorHandler as eh

from odpw.timer import Timer
import argparse

import logging
logger = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()



from multiprocessing.dummy import Pool as ThreadPool 
from functools import partial
import urlnorm

def head (dbm, sn, resource):
    try:
        props={}
        props['mime']=None
        props['size']=None
        props['redirects']=None
        props['status']=None
        props['header']=None
        try:
            props=util.head(resource.url)
        except Exception as e:
            eh.handleError(log, "HEAD", exception=e, url=resource.url, snapshot=sn,exc_info=True)
            props['status']=util.getExceptionCode(e)
            props['exception']=str(type(e))+":"+str(e.message)
        
        resource.updateStats(props)
        dbm.updateResource(resource)
        
    except Exception as e:
        eh.handleError(log, "head function", exception=e, url=resource.url, snapshot=sn,exc_info=True)


def getResources(dbm, snapshot):
    resources =[]
    for res in dbm.getResourceWithoutHead(snapshot=snapshot):
        try:
            url=urlnorm.norm(res['url'])
            R = Resource.fromResult(dict(res))
            resources.append(R)    
        except Exception as e:
            log.debug('Drop head lookup', exctype=type(e), excmsg=e.message, url=url, snapshot=snapshot)
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
        resources=getResources(self.dbm, self.snapshot)
        
        checks=0
        while not self.exit.is_set() or len(resources) != 0:

            log.debug("Starting head lookups", count=len(resources), cores=self.processors)
    
            pool = ThreadPool(processes=self.processors,) 
    
            head_star = partial(head, self.dbm, self.snapshot)
            results = pool.map(head_star, resources)
            pool.close()
            
            pool.join()
            
            resources=getResources(self.dbm, self.snapshot)
            checks+=1
            sleep(60)
            if checks % 15==0:
                log.info("Head lookup check", count=len(resources), cores=self.processors) 
            
            
            
            
    def shutdown(self):
        print "Shutdown initiated"
        self.exit.set()
    def setProcessors(self, processors):
        self.processors=processors

def name():
    return 'Head'

def setupCLI(pa):
    pa.add_argument("--force", action='store_true', help='force a full fetch, otherwise use update',dest='fetch')
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=1)

def cli(args,dbm):

    sn = getSnapshot(args)
    if not sn:
        return

    resources=[]
    for res in dbm.getResourceWithoutHead(snapshot=sn, status=600):
        try:
            url=urlnorm.norm(res['url'])
            R = Resource.fromResult(dict(res))
            resources.append(R)    
        except Exception as e:
            log.debug('Drop head lookup', exctype=type(e), excmsg=e.message, url=url, snapshot=sn)

    
    log.info("Starting head lookups", count=len(resources), cores=args.processors)
    
    pool = ThreadPool(processes=args.processors,) 
    
    head_star = partial(head, dbm, sn)
    results = pool.map(head_star, resources)
    pool.close()
    pool.join()
    
    Timer.printStats()
    log.info("Timer", stats=Timer.getStats())
    
    eh.printStats()

    #dbm.updateTimeInSnapshotStatusTable(sn=sn, key="fetch_end")