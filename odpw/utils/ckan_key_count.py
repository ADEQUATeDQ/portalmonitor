from odpw.analysers.count_analysers import DatasetCount, DCATDistributionCount,\
    DCATOrganizationsCount, DCATTagsCount, DCATFormatCount, ResourceCount
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
from odpw.utils.fetch import checkProcesses
import os
import pickle

__author__ = 'jumbrich'


from datetime import datetime
from multiprocessing.process import Process
import time 

#from odpw.utils.head import HeadProcess
from odpw.db.models import Portal, PortalMetaData, Dataset
import odpw.utils.util as util
from odpw.utils.util import getSnapshot,getExceptionCode,ErrorHandler as eh,\
    getExceptionString, TimeoutError, progressIterator

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
    log.info("START CKANMetrics", pid=Portal.id, sn=sn, fullfetch=fullfetch, software=Portal.software)

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

        try:
            iter = Dataset.iter(dbm.getDatasetsAsStream(portalID=Portal.id, snapshot=sn))
            process_all( ae, iter)
        except TimeoutError as exc:
            eh.handleError(log, "TimeoutError", exception=exc, pid=Portal.id, snapshot=sn, exc_info=True)
            ae.done()
            pmd.fetchTimeout(exc.timeout)

        ae.update(pmd)
        # store to DB
        dbm.updatePortalMetaData(pmd)
        
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
        # store to DB
        dbm.updatePortalMetaData(pmd)
        
        
        #import pprint
        #pprint.pprint(pmd.qa_stats)
        #Qu(core), Qu(res), Qu(extra), Qc(core), Qc(res), Qc(extra), Qo(F), Qo(L), Qa(url), Qa(email), Qr(ds), Qr(res)
        p=[pmd.portal_id,
           -1, # TODO str(pmd.qa_stats['Qu']['core']),
           -1, # TODO str(pmd.qa_stats['Qu']['res']),
           -1, # TODO str(pmd.qa_stats['Qu']['extra']),

           str(pmd.qa_stats['Qc']['core']),
           str(pmd.qa_stats['Qc']['res']),
           str(pmd.qa_stats['Qc']['extra']),

           str(pmd.qa_stats['Qo']['format']),
           str(pmd.qa_stats['Qo']['license']),

           str(pmd.qa_stats['Qa']['url']['total']),
           str(pmd.qa_stats['Qa']['email']['total']),

           str(pmd.qa_stats['DatasetRetrievability']['DatasetRetrievability']['avgP']['qrd']),
           str(pmd.qa_stats['ResourceRetrievability']['ResourceRetrievability']['avgP']['qrd']),

           str(pmd.datasets),
           str(pmd.resources),
           ]
        s=",".join(p)
        outfile.write(s+"\n")

        
    except Exception as exc:
        eh.handleError(log, "UPDATE DB", exception=exc, pid=Portal.id, snapshot=sn, exc_info=True)

    log.info("END CKANMetrics", pid=Portal.id, sn=sn, fullfetch=fullfetch)
  
def name():
    return 'CKANKeyCount'
def help():
    return "Get a key count per CKAN portal "

def setupCLI(pa):
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")
    pa.add_argument('-o','--out',type=str, dest='output' , help="outputdir")
    
def cli(args,dbm):

    sn = getSnapshot(args)
    if not sn:
        return
    
    
    portals=[]
    if args.url:
        p = dbm.getPortal(apiurl=args.url)
        if p:
            portals.append(p)
    else:
        for p in Portal.iter(dbm.getPortals()):
            portals.append(p)
    
    res={}
    for p in portals:
        
        total=dbm.countDatasets(portalID=p.id, snapshot=sn)
        
        steps=total/10
        if steps ==0:
            steps=1
         
        ae= AnalyserSet()
        ka= ae.add(CKANKeyAnalyser())
        
        iter = Dataset.iter(dbm.getDatasets(portalID=p.id, snapshot=sn))
        process_all(ae, progressIterator(iter, total, steps, label=p.id))
    
        res[p.id]=ka.getResult()
    
        
        with open(os.path.abspath(os.path.join(args.output,p.id+'.pkl')), 'wb') as output_file:
            print 'Pickling to',output_file
            pickle.dump( ka.getResult(), output_file )
    
    with open(os.path.abspath(os.path.join(args.output,'ckan_key_counts.pkl')), 'wb') as output_file:
        print 'Pickling to',output_file
        pickle.dump( res, output_file )
        
    
    
