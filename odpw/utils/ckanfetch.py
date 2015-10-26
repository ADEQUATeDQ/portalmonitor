from odpw.analysers.count_analysers import DatasetCount, DCATDistributionCount,\
    DCATOrganizationsCount, DCATTagsCount, DCATFormatCount
from odpw.analysers.statuscodes import DatasetStatusCode, ResourceStatusCode
from odpw.analysers.dbm_handlers import DatasetInserter,\
    DCATDistributionInserter, DatasetFetchUpdater
from odpw.analysers.core import DCATConverter
from odpw.analysers.datasetlife import DatasetLifeAnalyser
from odpw.analysers.quality.analysers.completeness import CompletenessAnalyser
from odpw.analysers.quality.analysers.contactability import ContactabilityAnalyser
from odpw.analysers.quality.analysers.openness import OpennessAnalyser
from odpw.analysers.quality.analysers.opquast import OPQuastAnalyser
from odpw.analysers.quality.new.retrievability import DatasetRetrievability,\
    ResourceRetrievability
__author__ = 'jumbrich'


from datetime import datetime
from multiprocessing.process import Process
import time 

#from odpw.utils.head import HeadProcess
from odpw.db.models import Portal, PortalMetaData, Dataset
import odpw.utils.util as util
from odpw.utils.util import getSnapshot,getExceptionCode,ErrorHandler as eh,\
    getExceptionString, TimeoutError

from odpw.analysers import  AnalyserSet, process_all, SAFEAnalyserSet
from odpw.analysers.fetching import MD5DatasetAnalyser, DCATResourceInDSAge,\
    DCATDatasetAge, UsageAnalyser, CKANKeyAnalyser
from odpw.portal_processor import CKAN, Socrata, OpenDataSoft

import argparse

import structlog
log =structlog.get_logger()


def fetching(obj, outfile):
    Portal = obj['portal']
    sn=obj['sn']
    dbm=obj['dbm']
    fullfetch=obj['fullfetch']

    dbm.engine.dispose()
    log.info("START Fetching", pid=Portal.id, sn=sn, fullfetch=fullfetch, software=Portal.software)
    
    try:
        ## get the pmd for this job
        pmd = dbm.getPortalMetaData(portalID=Portal.id, snapshot=sn)
    except Exception as exc:
        eh.handleError(log, "GET PMD", exception=exc, pid=Portal.id, snapshot=sn, exc_info=True)
        return

    try: 
        ae = SAFEAnalyserSet()
        
        ae.add(MD5DatasetAnalyser())
        if Portal.software == 'CKAN':
            ka= ae.add(CKANKeyAnalyser())
            ae.add(CompletenessAnalyser())
            ae.add(ContactabilityAnalyser())
            ae.add(OpennessAnalyser())
            ae.add(UsageAnalyser(ka))
            #ae.add(OPQuastAnalyser())
            

            processor = CKAN()
        elif Portal.software == 'Socrata':
            processor = Socrata()
        elif Portal.software == 'OpenDataSoft':
            processor = OpenDataSoft()
        else:
            raise NotImplementedError(Portal.software + ' is not implemented')

        try:
            iter = Dataset.iter(dbm.getDatasetsAsStream(portalID=Portal.id, snapshot=sn))
            process_all( ae, iter)
        except TimeoutError as exc:
            eh.handleError(log, "TimeoutError", exception=exc, pid=Portal.id, snapshot=sn, exc_info=True)
            ae.done()
            pmd.fetchTimeout(exc.timeout)

        ae.update(pmd)
        
    except Exception as exc:
        eh.handleError(log, "PortalFetch", exception=exc, pid=Portal.id, snapshot=sn, exc_info=True)
        if pmd:
            pmd.fetch_stats['status'] = getExceptionCode(exc)
            pmd.fetch_stats['exception'] = getExceptionString(exc)
    try:
        dr=DatasetStatusCode()
        rr=ResourceStatusCode()
        rdq= DatasetRetrievability(dr)
        rrq=ResourceRetrievability(rr)
        
        rdq.analyse_PortalMetaData(pmd)
        rrq.analyse_PortalMetaData(pmd)
        
        rdq.update_PortalMetaData(pmd)
        rrq.update_PortalMetaData(pmd)
        
        
        import pprint 
        pprint.pprint(pmd.qa_stats)
        #Qu(core), Qu(res), Qu(extra), Qc(core), Qc(res), Qc(extra), Qo(F), Qo(L), Qa(url), Qa(email), Qr(ds), Qr(res)
        p=[pmd.portal_id,
           str(pmd.qa_stats['Qu']['core']),
           str(pmd.qa_stats['Qu']['res']),
           str(pmd.qa_stats['Qu']['extra']),
           
           str(pmd.qa_stats['Qc']['core']),
           str(pmd.qa_stats['Qc']['res']),
           str(pmd.qa_stats['Qc']['extra']),
           
           str(pmd.qa_stats['Qo']['format']),
           str(pmd.qa_stats['Qo']['license']),
           
           str(pmd.qa_stats['Qa']['url']['total']),
           str(pmd.qa_stats['Qa']['email']['total']),
           
           str(pmd.qa_stats['DatasetRetrievability']['DatasetRetrievability']['avgP']['qrd']),
           str(pmd.qa_stats['ResourceRetrievability']['ResourceRetrievability']['avgP']['qrd']),
           ]
        s=",".join(p)
        outfile.write(s+"\n")
        
        
    except Exception as exc:
        eh.handleError(log, "UPDATE DB", exception=exc, pid=Portal.id, snapshot=sn, exc_info=True)

    log.info("END Fetching", pid=Portal.id, sn=sn, fullfetch=fullfetch)
  
def name():
    return 'CKANFetch'
def help():
    return "Fetch portal data"

def setupCLI(pa):
    gfilter = pa.add_argument_group('filters', 'filter option')
    gfilter.add_argument('-d','--datasets',type=int, dest='ds', help='filter portals with more than specified datasets')
    gfilter.add_argument('-r','--resources',type=int, dest='res')
    gfilter.add_argument('-s','--software',choices=['CKAN'], dest='software')
    gfilter.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")

    pa.add_argument("--force", action='store_true', help='force a full fetch, otherwise use update',dest='fetch')
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=1)
    pa.add_argument("-o","--csv", type=argparse.FileType('w'), dest="outfile")

def cli(args,dbm):

    sn = getSnapshot(args)
    if not sn:
        return
    
    jobs=[]
    fetch=True
    if args.fetch:
        fetch=True
    
    if args.url:
        p = dbm.getPortal( apiurl=args.url)
        pmd = dbm.getPortalMetaData(portalID=p.id, snapshot=sn)
        if not pmd:
            pmd = PortalMetaData(portalID=p.id, snapshot=sn)
            dbm.insertPortalMetaData(pmd)
            
        log.info("Queuing", pid=p.id, apiurl=args.url)
        jobs.append( {'portal':p, 'sn':sn, 'dbm':dbm, 'fullfetch':fetch } )
    else:
        
        ps=[]
        for p in PortalMetaData.iter(dbm.getPortalMetaDatas(snapshot=1533)):
            ps.append(p.portal_id)
        
        
        pjobs={}
        for p in Portal.iter(dbm.getPortals(software="CKAN")):
            if p.id in ps:
                pjobs[p.id]={'portal':p, 'sn':sn, 'dbm':dbm, 'fullfetch':fetch }
                
        
        import collections
        od = collections.OrderedDict(sorted(pjobs.items()))
        
        for p, job in od.items():
            log.info("Queuing", pid=p)
            jobs.append( job )        
    try:
        log.info("Start processing", portals=len(jobs), processors=args.processors, start = time.time())
        
             #Qu(core), Qu(res), Qu(extra), Qc(core), Qc(res), Qc(extra), Qo(F), Qo(L), Qa(url), Qa(email), Qr(ds), Qr(res)
        p=["portal_id",
           'Qu(core)',
           'Qu(res)',
           'Qu(extra)',
           
           'Qc(core)',
           'Qc(res)',
           'Qc(extra)',
           
           'Qo(format)',
           'Qo(license)',
           
           
           'Qa(url)'
           'Qa(email)',
           
           'Qr(ds)',
           'Qr(res)'
           ]
        s=",".join(p)
        args.outfile.write(s+"\n")
        
        for job in jobs:
            
            fetching(job, args.outfile)
        
        
    except Exception as e:
        eh.handleError(log, "ProcessingFetchException", exception=e, exc_info=True) 