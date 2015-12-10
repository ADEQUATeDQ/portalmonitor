
import time
from odpw.analysers.quality.analysers import dcat_analyser
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
from odpw.analysers.statuscodes import DatasetStatusCode
from odpw.analysers.count_analysers import DatasetCount, DCATDistributionCount,\
    DCATLicenseCount, DCATOrganizationsCount, DCATTagsCount, DCATFormatCount, CKANLicenseIDCount, CKANTagsCount, \
    CKANFormatCount, CKANLicenseCount, CKANOrganizationsCount
from odpw.analysers.dbm_handlers import DatasetFetchUpdater,\
    DCATDistributionInserter, DatasetUpdater
from odpw.analysers import AnalyserSet
from multiprocessing.pool import ThreadPool
from _functools import partial
from odpw.analysers.datasetlife import DatasetLifeAnalyser
__author__ = 'jumbrich'

from odpw.utils.util import getSnapshot,getExceptionCode,ErrorHandler as eh,\
    progressIterator, progressIndicator, ErrorHandler

from multiprocessing.process import Process
import multiprocessing
from odpw.db.models import Portal,  PortalMetaData, Dataset

import structlog
log =structlog.get_logger()


rerun=[
       
       #"http://scisf.opendatasoft.com",
       #"http://dataratp.opendatasoft.com",
       #"http://pod.opendatasoft.com",
       #"http://public.opendatasoft.com",

       
       
#===============================================================================
#        "http://opendata.comune.bari.it/",
# "https://offenedaten.de/",
# "http://datosabiertos.malaga.eu/",
# "http://datagm.org.uk/",
# "http://catalog.data.ug/",
# "http://ckan.data.ktn.gv.at/",
# "http://datos.argentina.gob.ar/",
# "http://hubofdata.ru/",
# "https://www.data.vic.gov.au/",
# "http://data.salzburgerland.com/",
# "https://opendata.bayern.de/",
# "http://data.gov.ie/",
# "http://ckan.okfn.gr/",
# "http://www.opendata-hro.de/",
# "http://data.gov.au/",
# "http://opingogn.is/",
# "http://data.openpolice.ru/",
# "http://datospublicos.org/",
# "http://leedsdatamill.org/",
# "http://opendata.lisra.jp/",
# "https://catalogodatos.gub.uy/",
# "https://data.stadt-zuerich.ch/",
# "https://beta.avoindata.fi/data/fi/",
# "http://data.nhm.ac.uk/",
# "http://dados.recife.pe.gov.br/",
# "http://www.edinburghopendata.info/",
# "http://drdsi.jrc.ec.europa.eu/",
# "http://africaopendata.org/",
# "http://linkeddatacatalog.dws.informatik.uni-mannheim.de/",
# "http://dartportal.leeds.ac.uk/",
# "http://data.opencolorado.org/",
# "http://opendata.opennorth.se/",
# "http://data.opencolorado.org/",
# "http://data.grcity.us/",
# "https://data.overheid.nl/data/",
# "http://data.glasgow.gov.uk/",
# "http://opendata.awt.be/",
# "http://www.odaa.dk/",
# "http://www.datos.misiones.gov.ar/",
# "http://dados.gov.br/",
# "http://iatiregistry.org/",
# "http://bermuda.io/",
# "http://data.amsterdamopendata.nl/",
# "http://data.cityofsantacruz.com/",
# "http://datahub.io/",
# "http://opendata.admin.ch/",
#===============================================================================
]

def simulateFetching(dbm, job):
    
    Portal = job['Portal']
    sn = job['snapshot']
    
    try:
        
        log.info("START Simulated Fetch", pid=Portal.id, snapshot=sn, software=Portal.software)
        dbm.engine.dispose()
        
        pmd = dbm.getPortalMetaData(portalID=Portal.id, snapshot=sn)
        if not pmd:
            pmd = PortalMetaData(portalID=Portal.id, snapshot=sn)
            dbm.insertPortalMetaData(pmd)
        
        ae = SAFEAnalyserSet()
        #ae.add(MD5DatasetAnalyser())
        ae.add(DatasetCount())
        #ae.add(DatasetStatusCode())
        
        if Portal.software == 'CKAN':
            #ka= ae.add(CKANKeyAnalyser())
            #ae.add(CKANLicenseIDCount())
            #ae.add(CKANResourceInDSAge())
            #ae.add(CKANDatasetAge())
            #ae.add(CKANFormatCount())
            #ae.add(CKANTagsCount())
            #ae.add(CKANLicenseCount())
            #ae.add(CKANOrganizationsCount())
            #ae.add(CompletenessAnalyser())
            #ae.add(ContactabilityAnalyser())
            #ae.add(OpennessAnalyser())
            #ae.add(OPQuastAnalyser())
            #ae.add(UsageAnalyser(ka))
            pass
        elif Portal.software == 'Socrata':
            pass
        elif Portal.software == 'OpenDataSoft':
            pass
                
        ae.add(DCATConverter(Portal))
        ae.add(DCATDistributionCount(withDistinct=True))
        #ae.add(DCATDistributionInserter(dbm))

        #ae.add(DatasetFetchUpdater(dbm))
        #ae.add(DatasetLifeAnalyser(dbm))

        ae.add(DCATOrganizationsCount())
        ae.add(DCATTagsCount())
        ae.add(DCATFormatCount())
        ae.add(DCATLicenseCount())
        ae.add(DCATResourceInDSAge())
        ae.add(DCATDatasetAge())

        # DCAT analyser
        for a in dcat_analyser():
            ae.add(a)

        total=dbm.countDatasets(portalID=Portal.id, snapshot=sn)
        
        steps=total/10
        if steps ==0:
            steps=1
        
        iter = Dataset.iter(dbm.getDatasets(portalID=Portal.id, snapshot=sn))
        process_all(ae, progressIterator(iter, total, steps, label=Portal.id))

        ae.update(pmd)
        dbm.updatePortalMetaData(pmd)
        
        log.info("DONE Simulated Fetch", pid=Portal.id, snapshot=sn)
    except Exception as e:
        ErrorHandler.handleError(log, "SimulateFetchException", portal=Portal.id, snapshot=sn)

def help():
    return "Simulate a fetch run"

def name():
    return 'FetchSim'

def setupCLI(pa):
    
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=1)
    pa.add_argument("-ns","--nosnap",  help='no snapshot', dest='snapshotignore', action='store_true')
    
def cli(args,dbm):
    sn = getSnapshot(args)
    
    portals=[]
    if args.url:
        p = dbm.getPortal(apiurl=args.url)
        if p:
            portals.append(p)
    elif len(rerun)>0:
        for apiurl in rerun:
            p = dbm.getPortal(apiurl=apiurl)
            if p:
                portals.append(p)
    else:
        for p in Portal.iter(dbm.getPortals()):
            portals.append(p)
    
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
    
    log.info("Starting fetch sim lookups", count=len(portals), cores=args.processors)
    
    
    head_star = partial(simulateFetching, dbm)
    
    start = time.time()
    results = pool.imap_unordered(head_star, jobs)
    pool.close()
    
    c=0
    total=len(jobs)
    steps= total/100 if total/100 !=0 else 1
    
    #for res in results:
    #    c+=1
    #    if c%steps==0:
    #        elapsed = (time.time() - start)
    #        progressIndicator(c, total, elapsed=elapsed, label="Fetch Simulate Progress")
   
    #progressIndicator(c, total, elapsed=elapsed, label="Fetch Simulate Progress")
    pool.join()
