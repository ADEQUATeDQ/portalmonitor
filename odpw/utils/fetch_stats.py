
import time
import odpw.utils.util as util
from odpw.analysers import AnalyseEngine

from odpw.analysers.core import DCATConverter
from odpw.analysers.fetching import MD5DatasetAnalyser
from odpw.analysers.statuscodes import DatasetStatusCount
from odpw.analysers.count_analysers import DatasetCount, DCATDistributionCount
from odpw.analysers.dbm_handlers import DatasetFetchUpdater,\
    DCATDistributionInserter
__author__ = 'jumbrich'

from odpw.utils.util import getSnapshot,getExceptionCode,ErrorHandler as eh,\
    progressIterator


from odpw.db.models import Portal,  PortalMetaData, Dataset

import logging
log = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()

def simulateFetching(dbm, Portal, sn):
    
    log.info("START Simulated Fetch", pid=Portal.id, snapshot=sn)
    dbm.engine.dispose()
    
    pmd = dbm.getPortalMetaData(portalID=Portal.id, snapshot=sn)
    if not pmd:
        pmd = PortalMetaData(portal=Portal.id, snapshot=sn)
        dbm.insertPortalMetaData(pmd)
     
    pmd.fetchstart()
    dbm.updatePortalMetaData(pmd)
    
    
    ae = AnalyseEngine()
    ae.add(MD5DatasetAnalyser())
    ae.add(DatasetCount())
    ae.add(DatasetStatusCount())
    
    ae.add(DCATConverter(Portal))
    ae.add(DCATDistributionCount(withDistinct=True))
    ae.add(DCATDistributionInserter(dbm))



#    ae.add(CKANResourceInDSAge())
#    ae.add(CKANDatasetAge())
#    ae.add(CKANKeyAnalyser())
#    ae.add(CKANFormatCount())


#    ae.add(CompletenessAnalyser())
#    ae.add(ContactabilityAnalyser())
#    ae.add(OpennessAnalyser())
#    ae.add(OPQuastAnalyser())


#    ae.add(DatasetFetchUpdater(dbm))
    
    
    total=0
    for res in dbm.countDatasets(portalID=Portal.id, snapshot=sn):
        total=res[0]
    steps=total/10
    if steps ==0:
        steps=1
    
    
    iter = Dataset.iter(dbm.getDatasets(portalID=Portal.id, snapshot=sn))
    ae.process_all(progressIterator(iter, total, steps))
    
    ae.update(pmd)
    ae.update(Portal)
        #print analyser.name()
        #print analyser.getResult()
    
    
    #pmd.update(ae)
    #from pprint import pprint
    #pprint(pmd.__dict__)
    dbm.updatePortalMetaData(pmd)
    dbm.updatePortal(Portal)

    log.info("DONE Simulated Fetch", pid=Portal.id, snapshot=sn)

def name():
    return 'FetchStats'
def setupCLI(pa):
    
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")
    
def cli(args,dbm):
    sn = getSnapshot(args)
    if not sn:
        return
    
    portals=[]
    if args.url:
        p = dbm.getPortal(apiurl=args.url)
        portals.append(p)
    else:
        for res in dbm.getPortals():
            p = Portal.fromResult(dict(res))
            portals.append(p)
    
    for p in portals:
        simulateFetching(dbm,p,sn)
    
    
    
