
__author__ = 'jumbrich'

from util import getSnapshot,getExceptionCode,ErrorHandler as eh


from odpw.db.models import Portal,  PortalMetaData, Dataset, Resource
from pprint import  pprint

import logging
log = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()




def snapshotStats(dbm, sn):
    
    #===========================================================================
    # ae = AnalyseEngine()
    # 
    # #check for the status of the portal for this snapshot
    # ae.add(PortalMetaDataStatusAnalyser())
    # 
    # #anaylse the fetch stats
    # ae.add(PortalMetaDataFetchStatsAnalyser())
    # 
    # #anaylse the resource stats
    # ae.add(PortalMetaDataResourceStatsAnalyser())
    # #anaylse the quality assessment stats
    # #ae.add(PortalMetaDataQAStatsAnalyser())
    # 
    # portals = dbm.getPortalMetaDatas(snapshot=sn)
    # ae.process_all(PortalMetaData.iter(portals))
    # 
    # print "Portal Status codes"
    # p_status=ae.getAnalyser(PortalMetaDataStatusAnalyser).getResult() 
    # pprint(p_status)
    # 
    # print "Fetch status"
    # pmdfs = ae.getAnalyser(PortalMetaDataFetchStatsAnalyser)
    # pprint(pmdfs.getResult())
    # 
    # print "Resource/Head status"
    # pmdfs = ae.getAnalyser(PortalMetaDataResourceStatsAnalyser)
    # pprint(pmdfs.getResult())
    #===========================================================================
    pass
    
    

def name():
    return 'Stats'
def setupCLI(pa):
    
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')

def cli(args,dbm):
    sn = getSnapshot(args)
    if not sn:
        return
        
    snapshotStats(dbm,sn)
