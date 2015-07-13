"""
Iterates over all packages for a given portal and computes basic statistics about used keys,

Creates the PortalMetaData object and for each dataset a metrics object


TODO: consider content for keys with dictionary values ( e.g., organisation)
"""
from odpw.db.models import Portal, Dataset, PortalMetaData, Resource

import util
from odpw.util import getSnapshot, getExceptionCode,ErrorHandler as eh

from odpw.timer import Timer
import argparse

import logging
import pprint
logger = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()



from odpw.quality.analysers.key_analyser import KeyAnalyser
from odpw.quality.analysers.retrievability import RetrievabilityAnalyser
from odpw.quality.analysers.completeness import CompletenessAnalyser
from odpw.quality.analysers.contactability import ContactabilityAnalyser
from odpw.quality.analysers.usage import UsageAnalyser
from odpw.quality.analysers.openness import OpennessAnalyser
from odpw.quality.analysers.opquast import OPQuastAnalyser



#def snapshot_metrics(pmd, portal, package, snapshot, dbm):
#    dataset_metrics = dbm.getSingleDatasetMetrics(portal, package, snapshot)
#    if dataset_metrics is None:
#        dataset_metrics = DatasetMetrics(pmd=pmd, package=package)
#    dbm.storeDatasetMetrics(dataset_metrics)


def packageQuality(portal, snapshot, dbm):
    """
    """
    global logger
    logger = logging.getLogger(__name__)
    logger.info("(%s) Getting meta data statistics for the packages", portal.url)

    # retrieve the PMD object and reset all its values
    PMD = dbm.getPortalMetaData(portal.url, snapshot)
    PMD.reset()

    c = 0

    analysers = [
        KeyAnalyser(),
        RetrievabilityAnalyser(dbm, portal.id, snapshot),
        CompletenessAnalyser(),
        ContactabilityAnalyser(),
        #OpennessAnalyser(),
        OPQuastAnalyser()
    ]

    datasets = []
    #if items['time'] == snapshot)
    for dataset in dbm.getPackages(portal, snapshot=snapshot):
        datasets.append(dataset)

        for analyser in analysers:
            try:
                analyser.visit(dataset)
            except Exception as e:
                logger.exception('Exception during quality analyser:')
                logger.exception(e)

        #snapshot_metrics(PMD, portal, Package(dict_string=dataset), snapshot, dbm)
        c += 1

    uA = UsageAnalyser(analysers[0])
    for ds in datasets:
        uA.visit(ds)

    #add UA to analyser array
    analysers.append(uA)

    logger.info("(%s) stored dataset metrics", portal.url)

    for analyser in analysers:
        analyser.computeSummary()
        analyser.update(PMD)

    dbm.storePortalMetaData(PMD)
    dbm.storePortal(portal)
    logger.info("(%s) analysed %s packages", portal.url, c)


def analyseQuality(pmd, dbm, datasets=False, resources=False):
    analysers=[]
    if datasets:
        analysers.append(KeyAnalyser())
        analysers.append(CompletenessAnalyser())
        analysers.append(ContactabilityAnalyser())
        analysers.append(OpennessAnalyser())
        analysers.append(OPQuastAnalyser())
        analysers.append(UsageAnalyser(pmd.general_stats['keys']))
                         
    if resources:
        analysers.append(RetrievabilityAnalyser(dbm, pmd.portal, pmd.snapshot))
    
    
    for ds in dbm.getDatasets(portalID= pmd.portal, snapshot=pmd.snapshot):
        dataset = Dataset.fromResult(dict(ds))
        for analyser in analysers:
            try:
                analyser.visit(dataset)
            except Exception as e:
                logger.exception('Exception during quality analyser:')
                logger.exception(e)
        
    
    for analyser in analysers:
        analyser.computeSummary()
        analyser.update(pmd)

    import pprint
    pprint.pprint(pmd.__dict__)
    dbm.updatePortalMetaData(pmd)

                
def name():
    return 'Quality'

def setupCLI(pa):
    pa.add_argument('-d','--datasets', dest='datasets',action='store_true')
    pa.add_argument('-r','--resources', dest='resources',action='store_true')
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-p","--portal",  help='specify portal id', dest='portal')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    
def cli(args,dbm):
    sn = getSnapshot(args)
    if not sn:
        return
    
    
    if args.portal:
        pmd = dbm.getPortalMetaData(portalID= args.portal, snapshot=sn)
        analyseQuality(pmd, dbm, datasets=args.datasets, resources=args.resources)
        
    else:
        for res in dbm.getPortalMetaDatas(snapshot=sn):
            pmd = PortalMetaData.fromResult(dict(res))
        
            analyseQuality(pmd, dbm, datasets=args.datasets, resources=args.resources)
        
        
        
        
    
    
    
    
    
    
    