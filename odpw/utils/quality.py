
import time
import odpw.utils.util as util
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.core import DCATConverter
from odpw.analysers.quality.new.completeness_dcat import CompletenessDCATAnalyser

__author__ = 'jumbrich'

from odpw.utils.util import getSnapshot,getExceptionCode,ErrorHandler as eh,\
    progressIterator


from odpw.db.models import Portal,  PortalMetaData, Dataset, Resource
from pprint import  pprint

import structlog
log =structlog.get_logger()


def quality(dbm, sn, portal):
    total=0
    print portal.id,sn
    total= dbm.countDatasets(snapshot=sn, portalID=portal.id)
        
    log.info("Computing head lookup stats",sn=sn, count=total)
    steps= total/10 
    if steps==0:
        steps=1
    
    iter = progressIterator(Dataset.iter(dbm.getDatasets(snapshot=sn, portalID=portal.id, limit=1)), total, steps)
        
    aset = AnalyserSet()
    
    dcat = aset.add(DCATConverter(portal))
    conf= aset.add(CompletenessDCATAnalyser())
    
    process_all(aset, iter)
    
    
    print conf.getResult()
    pmd = dbm.getPortalMetaData(snapshot=sn, portalID=portal.id)
    
    aset.update(pmd)
    
    print pmd.qa_stats
    
    #dbm.updatePortalMetaData(pmd)

def help():
    return "Compute quality stats"
def name():
    return 'Quality'
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
            pids.append(p)
    else:
        for p in Portal.iter(dbm.getPortals()):
            pids.append(p)
    for p in pids:
        snapshots=set([])
        if not sn:
            for s in dbm.getSnapshots(portalID=p.id):
                snapshots.add(s['snapshot'])
        else:
            snapshots.add(sn)
        
        for sn in sorted(snapshots):
            quality(dbm,sn,p)
