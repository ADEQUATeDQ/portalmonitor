from odpw.analysers import AnalyseEngine, QualityAnalyseEngine
from odpw.analysers.fetching import MD5DatasetAnalyser, DatasetCount,\
    CKANResourceInDS, CKANResourceInserter, DatasetStatusCount, CKANResourceInDSAge,\
    CKANDatasetAge, CKANKeyAnalyser, CKANFormatCount, DatasetFetchUpdater, DatasetFetchInserter
from odpw.analysers.quality.analysers.completeness import CompletenessAnalyser
from odpw.analysers.quality.analysers.contactability import ContactabilityAnalyser
from odpw.analysers.quality.analysers.openness import OpennessAnalyser
from odpw.analysers.quality.analysers.opquast import OPQuastAnalyser
from odpw.portal_processor import CKAN, Socrata, OpenDataSoft

__author__ = 'jumbrich'


from datetime import datetime
from multiprocessing.process import Process
from time import sleep
from odpw.head import HeadProcess


from odpw.db.models import Portal, PortalMetaData, Dataset

import odpw.util as util
from odpw.util import getSnapshot,getExceptionCode,ErrorHandler as eh

import argparse

import logging
logger = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()






def fetching(obj):
    Portal = obj['portal']
    sn=obj['sn']
    dbm=obj['dbm']
    fullfetch=obj['fullfetch']

    dbm.engine.dispose()
    
    try:
        log.info("START Fetching", pid=Portal.id, sn=sn, fullfetch=fullfetch, software=Portal.software)
        pmd = dbm.getPortalMetaData(portalID=Portal.id, snapshot=sn)
        if not pmd:
            pmd = PortalMetaData(portal=Portal.id, snapshot=sn)
            dbm.insertPortalMetaData(pmd)
        pmd.fetchstart()
        dbm.updatePortalMetaData(pmd)


        if Portal.software == 'CKAN':

            ae = AnalyseEngine()

            ae.add(MD5DatasetAnalyser())
            ae.add(DatasetCount())
            ae.add(CKANResourceInDS(withDistinct=True))
            ae.add(CKANResourceInserter(dbm))
            ae.add(DatasetStatusCount())
            ae.add(CKANResourceInDSAge())
            ae.add(CKANDatasetAge())
            ae.add(CKANKeyAnalyser())
            ae.add(CKANFormatCount())

            ae.add(CompletenessAnalyser())
            ae.add(ContactabilityAnalyser())
            ae.add(OpennessAnalyser())
            ae.add(OPQuastAnalyser())

            ae.add(DatasetFetchInserter(dbm))

            processor = CKAN(ae)
        elif Portal.software == 'Socrata':
            ae = AnalyseEngine()
            ae.add(DatasetCount())
            ae.add(MD5DatasetAnalyser())
            ae.add(DatasetFetchInserter(dbm))

            processor = Socrata(ae)
        elif Portal.software == 'OpenDataSoft':
            ae = AnalyseEngine()
            ae.add(DatasetCount())
            ae.add(MD5DatasetAnalyser())
            ae.add(DatasetFetchInserter(dbm))

            processor = OpenDataSoft(ae)
        else:
            raise NotImplementedError(Portal.software + ' is not implemented')

        processor.fetching(Portal, sn)

        pmd.fetchend()

        ae.update(pmd)
        ae.update(Portal)

        dbm.updatePortalMetaData(pmd)

    except Exception as exc:
        eh.handleError(log, "PortalFetch", exception=exc, pid=Portal.id, snapshot=sn,exc_info=True)
        Portal.status=getExceptionCode(exc)
        Portal.exception=str(type(exc))+":"+str(exc.message)

    dbm.updatePortal(Portal)
    log.info("END Fetching", pid=Portal.id, sn=sn, fullfetch=fullfetch, datasets=Portal.datasets)



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
                    log.info("FIN", PID= process.pid, portalID=portalID, apiurl=apiurl, start=start.isoformat(), exitcode=process.exitcode)
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

    pa.add_argument("--force", action='store_true', help='force a full fetch, otherwise use update',dest='fetch')
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=1)
    pa.add_argument("-o","--pidfile", type=argparse.FileType('w'), dest="outfile")

def cli(args,dbm):

    sn = getSnapshot(args)
    if not sn:
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
        
        #headProcess = HeadProcess(dbm, sn)
        #headProcess.start()
        
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
                
                util.progressIndicator(c, total)
            
            while len(processes) >0 :
                checkProcesses(processes,pidFile)
                checks+=1
                sleep(10)
                if checks % 90==0:
                    print "Status(",checks,"): cur:",len(processes),"pids, done:", (c-len(processes))
            
            util.progressIndicator(c, total)
        
        #headProcess.shutdown()        
        #headProcess.join()
        
        #log.info("RestartHeadLookups")
        #headProcess = HeadProcess(dbm, sn)
        #headProcess.start()
        #headProcess.shutdown()
        #headProcess.join()
        
    except Exception as e:
        eh.handleError(log, "ProcessingFetchException", exception=e, exc_info=True) 