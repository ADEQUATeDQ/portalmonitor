__author__ = 'jumbrich'

from db.models import Portal, Dataset, PortalMetaData, Resource

import util
from util import getSnapshot,getExceptionCode,ErrorHandler as eh

from timer import Timer
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