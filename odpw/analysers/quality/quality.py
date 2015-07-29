"""
Iterates over all packages for a given portal and computes basic statistics about used keys,

Creates the PortalMetaData object and for each dataset a metrics object


TODO: consider content for keys with dictionary values ( e.g., organisation)
"""
from odpw.db.models import Portal, Dataset, PortalMetaData, Resource

from odpw.utils.util import getSnapshot, getExceptionCode,ErrorHandler as eh

from odpw.utils.timer import Timer
import argparse

import logging
import pprint
import odpw.utils.util as util
from odpw.analysers import AnalyseEngine

from odpw.analysers.quality.analysers.completeness import CompletenessAnalyser
from odpw.analysers.quality.analysers.contactability import ContactabilityAnalyser
from odpw.analysers.quality.analysers.openness import OpennessAnalyser
from odpw.analysers.quality.analysers.opquast import OPQuastAnalyser
from odpw.analysers.quality.analysers.usage import UsageAnalyser
from odpw.analysers.quality.analysers.retrievability import RetrievabilityAnalyser
logger = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()




def analyseQuality(portal,snapshot, dbm, datasets=False, resources=False):
    
    pmd = dbm.getPortalMetaData(portalID= portal.id, snapshot=snapshot)
    if not pmd:
        print "no pmd for", portal.id, "and", snapshot
    
    ae = AnalyseEngine
    if datasets:
        
        ae.add(CompletenessAnalyser())
        ae.add(ContactabilityAnalyser())
        ae.add(OpennessAnalyser())
        ae.add(OPQuastAnalyser())
        ae.add(UsageAnalyser(pmd.general_stats['keys']))
                         
    if resources:
        ae.add(RetrievabilityAnalyser(dbm, pmd.portal, pmd.snapshot))
    
    total=0
    for res in dbm.countDatasets(portalID=portal.id, snapshot=snapshot):
        total=res[0]
    c=0
    steps=total/10
    if steps ==0:
        steps=1
    
    
    iter = Dataset.iter(dbm.getDatasets(portalID=Portal.id, snapshot=snapshot))
    for ds in iter:
        c+=1
        ae.analyse(ds)
        if c%steps == 0:
            util.progressIndicator(c, total)
        
    
    for ae in ae.getAnalysers():
        ae.update(pmd)
        ae.update(Portal)
    
    dbm.updatePortalMetaData(pmd)
    dbm.updatePortal(Portal)
    
                
def name():
    return 'Quality'

def setupCLI(pa):
    pa.add_argument('-d','--datasets', dest='datasets',action='store_true')
    pa.add_argument('-r','--resources', dest='resources',action='store_true')
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-u","--apiurl",  help='specify portal api url', dest='url')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    
def cli(args,dbm):
    sn = getSnapshot(args)
    
    portals=[]
    if args.url:
        p = dbm.getPortal(apiurl=args.url)
        portals.append(p)
    else:
        for res in dbm.getPortals():
            p = Portal.fromResult(dict(res))
            portals.append(p)
    
    
    for portal in portals:
        snapshots=[]
        if not sn:
            for s in dbm.getSnapshots(portalID=portal.id):
                snapshots.append(s['snapshot'])
        else:
            snapshots.append(sn)
            
        for sn in snapshots:
            analyseQuality(portal, sn, dbm, datasets=args.datasets, resources=args.resources)
    
    
        
        
        
        
        
    
    
    
    
    
    
    