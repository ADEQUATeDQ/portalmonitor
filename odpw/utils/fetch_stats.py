
import time
from odpw.analysers.quality.analysers.completeness import CompletenessAnalyser
from odpw.analysers.quality.analysers.contactability import ContactabilityAnalyser
from odpw.analysers.quality.analysers.openness import OpennessAnalyser
from odpw.analysers.quality.analysers.opquast import OPQuastAnalyser
import odpw.utils.util as util
from odpw.analysers import AnalyseEngine, process_all, SAFEAnalyserSet

from odpw.analysers.core import DCATConverter
from odpw.analysers.fetching import MD5DatasetAnalyser, DCATDatasetAge,\
    DCATResourceInDSAge, CKANKeyAnalyser, CKANDatasetAge, CKANResourceInDSAge,\
    UsageAnalyser
from odpw.analysers.statuscodes import DatasetStatusCount
from odpw.analysers.count_analysers import DatasetCount, DCATDistributionCount,\
    DCATLicenseCount, DCATOrganizationsCount, DCATTagsCount, DCATFormatCount, CKANLicenseIDCount, CKANTagsCount, \
    CKANFormatCount, CKANLicenseCount, CKANOrganizationsCount
from odpw.analysers.dbm_handlers import DatasetFetchUpdater,\
    DCATDistributionInserter, DatasetUpdater
from odpw.analysers import AnalyserSet
from multiprocessing.pool import ThreadPool
from _functools import partial
__author__ = 'jumbrich'

from odpw.utils.util import getSnapshot,getExceptionCode,ErrorHandler as eh,\
    progressIterator, progressIndicator

from multiprocessing.process import Process
import multiprocessing
from odpw.db.models import Portal,  PortalMetaData, Dataset

import structlog
log =structlog.get_logger()

def simulateFetching(dbm, job):
    Portal = job['Portal']
    sn = job['snapshot']
    
    log.info("START Simulated Fetch", pid=Portal.id, snapshot=sn, software=Portal.software)
    dbm.engine.dispose()
    
    pmd = dbm.getPortalMetaData(portalID=Portal.id, snapshot=sn)
    if not pmd:
        pmd = PortalMetaData(portalID=Portal.id, snapshot=sn)
        dbm.insertPortalMetaData(pmd)
     
    
    ae = SAFEAnalyserSet()
    ae.add(MD5DatasetAnalyser())
    ae.add(DatasetCount())
    ae.add(DatasetStatusCount())
    
    if Portal.software == 'CKAN':
        ka= ae.add(CKANKeyAnalyser())
        ae.add(CKANLicenseIDCount())
        #ae.add(CKANResourceInDSAge())
        #ae.add(CKANDatasetAge())
        #ae.add(CKANFormatCount())
        #ae.add(CKANTagsCount())
        ae.add(CKANLicenseCount())
        #ae.add(CKANOrganizationsCount())
        ae.add(CompletenessAnalyser())
        ae.add(ContactabilityAnalyser())
        ae.add(OpennessAnalyser())
        ae.add(OPQuastAnalyser())
        ae.add(UsageAnalyser(ka))
        
    elif Portal.software == 'Socrata':
        pass
    elif Portal.software == 'OpenDataSoft':
        pass
    
            
    ae.add(DCATConverter(Portal))
    ae.add(DCATDistributionCount(withDistinct=True))
    ae.add(DCATDistributionInserter(dbm))
    
    ae.add(DCATOrganizationsCount())
    ae.add(DCATTagsCount())
    ae.add(DCATFormatCount())
    ae.add(DCATResourceInDSAge())
    ae.add(DCATDatasetAge())

    ae.add(DatasetFetchUpdater(dbm))
    
    total=dbm.countDatasets(portalID=Portal.id, snapshot=sn)
    
    steps=total/10
    if steps ==0:
        steps=1
    
    iter = Dataset.iter(dbm.getDatasets(portalID=Portal.id, snapshot=sn))
    process_all(ae,progressIterator(iter, total, steps, label=Portal.id))
    
    ae.update(pmd)
    
    dbm.updatePortalMetaData(pmd)
    
    log.info("DONE Simulated Fetch", pid=Portal.id, snapshot=sn)

def help():
    return "Simulate a fetch run"

def name():
    return 'FetchSim'

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
    
    
    print len(portals)
    jobs=[]
    for p in portals:
        snapshots=set([])
        if not sn:
            for s in dbm.getSnapshots(portalID=p.id):
                print s['snapshot']
                snapshots.add(s['snapshot'])
        else:
            snapshots.add(sn)
        
        for sn in sorted(snapshots):
            jobs.append({'Portal':p, 'snapshot':sn})
            
    
    
    pool = ThreadPool(processes=args.processors,) 
    mgr = multiprocessing.Manager()
    seen = mgr.dict()
    
    #for pmd in PortalMetaData.iter(dbm.getPortalMetaDatas(snapshot=sn)):
    #    seen[pmd.portal_id]= {'resources':pmd.resources, 'processed':0}
    
    log.info("Starting fetch sim lookups", count=len(portals), cores=args.processors)
    
    
    head_star = partial(simulateFetching,dbm)
    
    start = time.time()
    results = pool.imap_unordered(head_star, jobs)
    pool.close()
    
    c=0
    total=len(jobs)
    steps= total/100 if total/100 !=0 else 1
    
    for res in results:
        c+=1
        if c%steps==0:
            elapsed = (time.time() - start)
            progressIndicator(c, total, elapsed=elapsed, label="Fetch Simulate Progress")
   
    progressIndicator(c, total, elapsed=elapsed, label="Fetch Simulate Progress")
    pool.join()
