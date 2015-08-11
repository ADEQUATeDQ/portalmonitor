
import time
from odpw.analysers.quality.analysers.completeness import CompletenessAnalyser
from odpw.analysers.quality.analysers.contactability import ContactabilityAnalyser
from odpw.analysers.quality.analysers.openness import OpennessAnalyser
from odpw.analysers.quality.analysers.opquast import OPQuastAnalyser
import odpw.utils.util as util
from odpw.analysers import AnalyseEngine, process_all, SAFEAnalyserSet

from odpw.analysers.core import DCATConverter
from odpw.analysers.fetching import MD5DatasetAnalyser, DCATDatasetAge,\
    DCATResourceInDSAge, CKANKeyAnalyser, CKANDatasetAge, CKANResourceInDSAge
from odpw.analysers.statuscodes import DatasetStatusCount
from odpw.analysers.count_analysers import DatasetCount, DCATDistributionCount,\
    DCATLicenseCount, DCATOrganizationsCount, DCATTagsCount, DCATFormatCount, CKANLicenseIDCount, CKANTagsCount, \
    CKANFormatCount, CKANLicenseCount, CKANOrganizationsCount
from odpw.analysers.dbm_handlers import DatasetFetchUpdater,\
    DCATDistributionInserter
from odpw.analysers import AnalyserSet
__author__ = 'jumbrich'

from odpw.utils.util import getSnapshot,getExceptionCode,ErrorHandler as eh,\
    progressIterator


from odpw.db.models import Portal,  PortalMetaData, Dataset

import structlog
log =structlog.get_logger()

def simulateFetching(dbm, Portal, sn):
    
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
        ae.add(CKANKeyAnalyser())
        ae.add(CKANLicenseIDCount())
        ae.add(CKANResourceInDSAge())
        ae.add(CKANDatasetAge())
        ae.add(CKANFormatCount())
        ae.add(CKANTagsCount())
        ae.add(CKANLicenseCount())
        ae.add(CKANOrganizationsCount())
        ae.add(CompletenessAnalyser())
        ae.add(ContactabilityAnalyser())
        ae.add(OpennessAnalyser())
        ae.add(OPQuastAnalyser())

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
    
    #ae.add(DCATLicenseCount())

    ae.add(DCATResourceInDSAge())
    ae.add(DCATDatasetAge())

#    ae.add(CKANKeyAnalyser())
#    ae.add(CompletenessAnalyser())
#    ae.add(ContactabilityAnalyser())
#    ae.add(OpennessAnalyser())
#    ae.add(OPQuastAnalyser())
    #ae.add(DatasetFetchUpdater(dbm))
    
    
    total=dbm.countDatasets(portalID=Portal.id, snapshot=sn)
    
    steps=total/10
    if steps ==0:
        steps=1
    
    
    iter = Dataset.iter(dbm.getDatasets(portalID=Portal.id, snapshot=sn))
    process_all(ae,progressIterator(iter, total, steps))
    
    ae.update(pmd)
    #ae.update(pmd1)
    #ae.update(Portal)
    
    #import pprint 
    #pprint.pprint(pmd.__dict__)
    #print "_"
    #pprint.pprint(pmd1.__dict__)  
    pmd.fetchend()  
    dbm.updatePortalMetaData(pmd)
    #dbm.updatePortal(Portal)

    log.info("DONE Simulated Fetch", pid=Portal.id, snapshot=sn)

def help():
    return "Simulate a fetch run"

def name():
    return 'FetchSim'

def setupCLI(pa):
    
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")
    
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
    
    
    print len(pids)
    for p in pids:
        snapshots=set([])
        if not sn:
            for s in dbm.getSnapshots(portalID=p.id):
                snapshots.add(s['snapshot'])
        else:
            snapshots.add(sn)
        
        for sn in sorted(snapshots):
            print p.id, sn
            simulateFetching(dbm,p,sn)
    
    
