from datetime import datetime
from multiprocessing.process import Process
from time import sleep
from odpw.head import HeadProcess
from urlparse import urlparse
from odpw.analysers import AnalyseEngine, QualityAnalyseEngine
from odpw.quality.analysers.key_analyser import KeyAnalyser
from odpw.analysers.fetching import MD5DatasetAnalyser, DatasetCount,\
    ResourceInDS, ResourceInserter, DatasetStatusCount, ResourceInDSAge,\
    DatasetAge, FormatCount
from odpw.quality.analysers.completeness import CompletenessAnalyser
from odpw.quality.analysers.contactability import ContactabilityAnalyser
from odpw.quality.analysers.openness import OpennessAnalyser
from odpw.quality.analysers.opquast import OPQuastAnalyser
__author__ = 'jumbrich'

from odpw.db.models import Portal, Dataset, PortalMetaData, Resource

import ckanapi
import odpw.util as util
from odpw.util import getSnapshot,getExceptionCode,ErrorHandler as eh

from odpw.timer import Timer
import argparse

import logging
logger = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()


import random
import time


def generateFetchDatasetIter(Portal, sn):
    
    api = ckanapi.RemoteCKAN(Portal.apiurl, get_only=True)
    start=0
    rows=1000000
    
    processed=set([])
    
    while True:
        response = api.action.package_search(rows=rows, start=start)
        #print Portal.apiurl, start, rows, len(processed)
        datasets = response["results"] if response else None
        if datasets:
            rows = len(datasets) if start==0 else rows
            start+=rows
            for datasetJSON in datasets:
                datasetID = datasetJSON['name']
                
                if datasetID not in processed:
                    data = datasetJSON
                    
                    d = Dataset(snapshot=sn,portal=Portal.id, dataset=datasetID, data=data)
                    d.status=200
                    
                    processed.add(datasetID)
                        
                    if len(processed) % 1000 == 0:
                        log.info("ProgressDSFetch", pid=Portal.id, processed=len(processed))
                    
                    yield d
                
        else:
            break
    try:
        package_list, status = util.getPackageList(Portal.apiurl)
        total=len(package_list)
    
        for entity in package_list:
            #WAIT between two consecutive GET requests
            if entity not in processed:
                processed.add(d.dataset)
                 
                time.sleep(random.uniform(0.5, 1))
                log.debug("GETMetaData", pid=Portal.id, did=entity)
                with Timer(key="fetchDS("+Portal.id+")") as t, Timer(key="fetchDS") as t1:
                    #fetchDataset(entity, stats, dbm, sn)
                    props={
                           'status':-1,
                           'data':None,
                           'exception':None
                           }
                    try:
                        resp = api.action.package_show(id=entity)
                        data = resp
                        util.extras_to_dict(data)
                        props['data']=data
                        props['status']=200
                    except Exception as e:
                        eh.handleError(log,'FetchDataset', exception=e,pid=Portal.id, did=entity,
                           exc_info=True)
                        props['status']=util.getExceptionCode(e)
                        props['exception']=str(type(e))+":"+str(e.message)
                 
                    d = Dataset(snapshot=sn,portal=Portal.id, dataset=entity, **props)
                    processed.add(d.dataset)
                         
                    if len(processed) % 1000 == 0:
                        log.info("ProgressDSFetch", pid=Portal.id, processed=len(processed))
                     
                    yield d
                    
    except Exception as e:
        if len(processed)==0:
            raise e

def fetching(obj):
    Portal = obj['portal']
    sn=obj['sn']
    dbm=obj['dbm']
    fullfetch=obj['fullfetch']

    dbm.engine.dispose()
    
    
    try:
        
        log.info("START Fetching", pid=Portal.id, sn=sn, fullfetch=fullfetch)

        pmd = dbm.getPortalMetaData(portalID=Portal.id, snapshot=sn)
        if not pmd:
            pmd = PortalMetaData(portal=Portal.id, snapshot=sn)
            dbm.insertPortalMetaData(pmd)
        pmd.fetchstart()
        dbm.updatePortalMetaData(pmd)

        ae = AnalyseEngine()
    
        ae.add(MD5DatasetAnalyser())
        ae.add(DatasetCount())
        ae.add(ResourceInDS(withDistinct=True))
        ae.add(ResourceInserter(dbm))
        ae.add(DatasetStatusCount())
        ae.add(ResourceInDSAge())
        ae.add(DatasetAge())
        ae.add(KeyAnalyser())
        ae.add(FormatCount())
    
        qae = QualityAnalyseEngine()
        qae.add(CompletenessAnalyser())
        qae.add(ContactabilityAnalyser())
        qae.add(OpennessAnalyser())
        qae.add(OPQuastAnalyser())
    
        main = AnalyseEngine()
        main.add(ae)
        main.add(qae)
        
        Portal.latest_snapshot=sn
    
        iter = generateFetchDatasetIter(Portal, sn)
        main.process_all(iter)
    
        for ae in main.getAnalysers():
            for analyser in ae:
                analyser.update(pmd)
                analyser.update(Portal)
       
        dbm.updatePortalMetaData(pmd)

        
        Portal.datasets= ae.getAnalyser('ds').getResult()['count']
        Portal.resources= ae.getAnalyser('res').getResult()['count']
    except Exception as exc:
        Portal.status=getExceptionCode(exc)
        Portal.exception=str(type(exc))+":"+str(exc.message)
        
    dbm.updatePortal(Portal)
    
    


    log.info("END Fetching", pid=Portal.id, sn=sn, fullfetch=fullfetch)




def checkProcesses(processes, pidFile):
    rem=[]
    p= len(processes)
    for portalID in processes.keys():
        (pid, process, start,apiurl) = processes[portalID]
        if not process.is_alive():
            process.join() # Allow tidyup
            status = process.exitcode
            end = datetime.now()
            
            rem.append(portalID) # Removed finished items from the dictionary
            try:
                if status ==0:
                    log.info("FIN", PID= process.pid, portalID=portalID, apiurl=apiurl, start=start, exitcode=process.exitcode)
                    pidFile.write("FIN\t %s \t %s \t %s \t %s (%s)\n"%(process.pid,process.exitcode,end, portalID, apiurl))
                else:
                    log.info("ABORT", PID= process.pid, portalID=portalID, apiurl=apiurl, start=start, exitcode=process.exitcode)
                    pidFile.write("ABORT\t %s \t %s \t %s \t %s (%s)\n"%(process.pid,process.exitcode,end, portalID, apiurl))
                pidFile.flush()
            except Exception as e:
                print e, e.message()
    
    for pID in rem:
        del processes[pID]
    assert p-len(processes) == len(rem) 
    return len(rem)

def name():
    return 'Fetch'

def setupCLI(pa):
    gfilter = pa.add_argument_group('filters', 'filter option')
    gfilter.add_argument('-d','--datasets',type=int, dest='ds', help='filter portals with more than specified datasets')
    gfilter.add_argument('-r','--resources',type=int, dest='res')
    gfilter.add_argument('-s','--software',choices=['CKAN'], dest='software')
    gfilter.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")

    getportals = pa.add_argument_group('Portal info', 'information about portals')
    getportals.add_argument('-o','--out_file',type=argparse.FileType('w'), dest='outfile', help='store portal list')
    getportals.add_argument('-p','--portals',action='store_true', dest='getPortals')
    
    pa.add_argument("--force", action='store_true', help='force a full fetch, otherwise use update',dest='fetch')
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=1)

def cli(args,dbm):

    sn = getSnapshot(args)
    if not sn:
        return

    if args.getPortals:
        if not args.outfile:
            log.warning("No outputfile defined")
            return
        
        with args.outfile as file:
            for portalRes in dbm.getUnprocessedPortals(snapshot=sn):
                p = Portal.fromResult(dict(portalRes))
                file.write(p.apiurl+" "+p.id+"\n")
        
        return
    
    jobs=[]
    fetch=True
    if args.fetch:
        fetch=True
    if args.url:
        p= dbm.getPortal(apiurl=args.url)
        log.info("Queuing", pid=p.id, datasets=p.datasets, resources=p.resources)
        jobs.append(
            {   'portal':p,
                'sn':sn,
                'dbm':dbm,
                'fullfetch':fetch
            }
        )
    else:
        for portalRes in dbm.getPortals():
        #for portalRes in dbm.getUnprocessedPortals(snapshot=sn):
            p = Portal.fromResult(dict(portalRes))
            log.info("Queuing", pid=p.id, datasets=p.datasets, resources=p.resources)
            jobs.append(
                {   'portal':p,
                    'sn':sn,
                    'dbm':dbm,
                    'fullfetch': fetch
                }
            )
        
    try:
        log.info("Start processing", portals=len(jobs), processors=args.processors)
        
        headProcess = HeadProcess(dbm, sn)
        headProcess.start()
        
        processes={}
        
        fetch_processors = args.processors
        
        checks=0
        p_done=0
        with args.outfile as pidFile:
            pidFile.write("STATUS\t PID \t ds \t start \t p_id \t p_url\n")
            pidFile.flush()
            
            total=len(jobs)
            c=0
            for job in jobs:
                p = Process(target=fetching, args=((job,)))
                p.start()
                c+=1
            
                start = datetime.now()
                processes[job['portal'].id]=(p.pid, p, start, job['portal'].apiurl)
                
                log.info("START", PID= p.pid, portalID=job['portal'].id, apiurl=job['portal'].apiurl, start=start, datasets=job['portal'].datasets)
                pidFile.write("START\t %s \t %s \t %s \t %s (%s)\n"%(p.pid, job['portal'].datasets,start, job['portal'].id, job['portal'].apiurl))
                pidFile.flush()
                
                while len(processes) >= fetch_processors:
                    p_done+=checkProcesses(processes, pidFile)
                    checks+=1
                    sleep(10)
                    if checks % 90==0:
                        print "Status(",checks,"): cur:",len(processes),"pids, done:", (c-len(processes))
                
                util.progressINdicator(c, total)
            
            while len(processes) >0 :
                checkProcesses(processes,pidFile)
                checks+=1
                sleep(10)
                if checks % 90==0:
                    print "Status(",checks,"): cur:",len(processes),"pids, done:", (c-len(processes))
            
            util.progressINdicator(c, total)
        
        headProcess.shutdown()        
        headProcess.join()
        
        log.info("RestartHeadLookups")
        headProcess = HeadProcess(dbm, sn)
        headProcess.start()
        headProcess.shutdown()
        headProcess.join()
        
    except Exception as e:
        eh.handleError(log, "ProcessingFetchException", exception=e, exc_info=True) 