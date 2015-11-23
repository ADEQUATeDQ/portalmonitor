from odpw.analysers.count_analysers import DatasetCount, DCATDistributionCount,\
    DCATOrganizationsCount, DCATTagsCount, DCATFormatCount
from odpw.analysers.statuscodes import DatasetStatusCode
from odpw.analysers.dbm_handlers import DatasetInserter,\
    DCATDistributionInserter, DatasetFetchUpdater
from odpw.analysers.core import DCATConverter
from odpw.analysers.datasetlife import DatasetLifeAnalyser

from odpw.analysers.quality.new.existence_dcat import *
from odpw.analysers.quality.new.conformance_dcat import *
from odpw.analysers.quality.new.open_dcat_format import IANAFormatDCATAnalyser,\
    FormatOpennessDCATAnalyser, FormatMachineReadableDCATAnalyser
from odpw.analysers.quality.new.open_dcat_license import LicenseOpennessDCATAnalyser
__author__ = 'jumbrich'


from datetime import datetime
from multiprocessing.process import Process
import time 

#from odpw.utils.head import HeadProcess
from odpw.db.models import Portal, PortalMetaData
import odpw.utils.util as util
from odpw.utils.util import getSnapshot,getExceptionCode,ErrorHandler as eh,\
    getExceptionString, TimeoutError

from odpw.analysers import  AnalyserSet, process_all, SAFEAnalyserSet
from odpw.analysers.fetching import MD5DatasetAnalyser, DCATResourceInDSAge,\
    DCATDatasetAge
from odpw.portal_processor import CKAN, Socrata, OpenDataSoft

import argparse

import structlog
log =structlog.get_logger()


def fetching(obj):
    Portal = obj['portal']
    sn=obj['sn']
    dbm=obj['dbm']
    fullfetch=obj['fullfetch']

    dbm.engine.dispose()
    log.info("START Fetching", pid=Portal.id, sn=sn, fullfetch=fullfetch, software=Portal.software)
    
    try:
        ## get the pmd for this job
        pmd = dbm.getPortalMetaData(portalID=Portal.id, snapshot=sn)
        pmd.fetchstart()
        dbm.updatePortalMetaData(pmd)

    except Exception as exc:
        eh.handleError(log, "GET PMD", exception=exc, pid=Portal.id, snapshot=sn, exc_info=True)
        return

    try: 
        ae = SAFEAnalyserSet()
        
        ae.add(MD5DatasetAnalyser())
        ae.add(DatasetCount())
        ae.add(DatasetStatusCode())

        #ae.add(DCATConverter(Portal))
        #ae.add(DCATDistributionCount(withDistinct=True))
        #ae.add(DCATDistributionInserter(dbm))
        if Portal.software == 'CKAN':
            
            # ae.add(CKANResourceInDSAge())
            #ae.add(CKANDatasetAge())
            #ae.add(CKANKeyAnalyser())
            #ae.add(CKANFormatCount())
            #ae.add(CKANTagsCount())
            #ae.add(CKANLicenseCount())
            #ae.add(CKANOrganizationsCount())

            #ae.add(CompletenessAnalyser())
            #ae.add(ContactabilityAnalyser())
            #ae.add(OpennessAnalyser())
            #ae.add(OPQuastAnalyser())

            processor = CKAN()
        elif Portal.software == 'Socrata':
            processor = Socrata()
        elif Portal.software == 'OpenDataSoft':
            processor = OpenDataSoft()
        else:
            raise NotImplementedError(Portal.software + ' is not implemented')

        ae.add(DCATConverter(Portal))
        ae.add(DCATDistributionCount(withDistinct=True))
        ae.add(DCATDistributionInserter(dbm))
    
        ae.add(DCATOrganizationsCount())
        ae.add(DCATTagsCount())
        ae.add(DCATFormatCount())
        ae.add(DCATResourceInDSAge())
        ae.add(DCATDatasetAge())
        ae.add(DatasetInserter(dbm))
        #ae.add(DatasetLifeAnalyser(dbm))
        
        ##################### EXISTS ######################################
        # ACCESS
        access = ae.add(AnyMetric([AccessUrlDCAT(), DownloadUrlDCAT()], id='ExAc'))
        # DISCOVERY
        discovery = ae.add(AverageMetric([DatasetTitleDCAT(), DatasetDescriptionDCAT(), DatasetKeywordsDCAT(), DistributionTitleDCAT(), DistributionDescriptionDCAT()], id='ExDi'))
        # CONTACT
        contact = ae.add(AnyMetric([DatasetContactDCAT(), DatasetPublisherDCAT()], id='ExCo'))
        # LICENSE
        rights = ae.add(ProvLicenseDCAT())
        # PRESERVATION
        preservation = ae.add(AverageMetric([DatasetAccrualPeriodicityDCAT(), DistributionFormatsDCAT(), DistributionMediaTypesDCAT(), DistributionByteSizeDCAT()], id='ExPr'))
        # DATE
        date = ae.add(AverageMetric([DatasetCreationDCAT(), DatasetModificationDCAT(), DistributionIssuedDCAT(), DistributionModifiedDCAT()], id='ExDa'))
        # TEMPORAL
        temporal = ae.add(DatasetTemporalDCAT())
        # SPATIAL
        spatial = ae.add(DatasetSpatialDCAT())
    
        ####################### CONFORMANCE ###########################
        # ACCESS
        accessUri = ae.add(AnyConformMetric([ConformAccessUrlDCAT(), ConformDownloadUrlDCAT()], id='CoAc'))
        # CONTACT
        contactEmail = ae.add(AnyConformMetric([EmailConformContactPoint(), EmailConformPublisher()], id='CoCE'))
        contactUri = ae.add(AnyConformMetric([UrlConformContactPoint(), UrlConformPublisher()], id='CoCU'))
    
        # DATE
        dateformat = ae.add(AverageConformMetric([DateConform(dcat_access.getCreationDate),
                                                 DateConform(dcat_access.getModificationDate),
                                                 DateConform(dcat_access.getDistributionCreationDates),
                                                 DateConform(dcat_access.getDistributionModificationDates)], id='CoDa'))
        # LICENSE
        licenseConf = ae.add(LicenseConform())
        # FORMAT
        formatConf = ae.add(IANAFormatDCATAnalyser())
    
        ####################### OPENNESS ###########################
        formatOpen = ae.add(FormatOpennessDCATAnalyser())
        formatMachine = ae.add(FormatMachineReadableDCATAnalyser())
        licenseOpen = ae.add(LicenseOpennessDCATAnalyser())

        try:
            iter = processor.generateFetchDatasetIter(Portal, sn)
            process_all( ae, iter)
        except TimeoutError as exc:
            eh.handleError(log, "TimeoutError", exception=exc, pid=Portal.id, snapshot=sn, exc_info=True)
            ae.done()
            pmd.fetchTimeout(exc.timeout)

        pmd.fetchend()
        #processor.fetching(Portal, sn)

        ae.update(pmd)
        ae.update(Portal)

        pmd.fetch_stats['status'] = 200
        pmd.fetch_stats['exception'] = None
        

    except Exception as exc:
        eh.handleError(log, "PortalFetch", exception=exc, pid=Portal.id, snapshot=sn, exc_info=True)
        if pmd:
            pmd.fetch_stats['status'] = getExceptionCode(exc)
            pmd.fetch_stats['exception'] = getExceptionString(exc)
        
    try:
        dbm.updatePortalMetaData(pmd)
        dbm.updatePortal(Portal)
        
    except Exception as exc:
        eh.handleError(log, "UPDATE DB", exception=exc, pid=Portal.id, snapshot=sn, exc_info=True)

    log.info("END Fetching", pid=Portal.id, sn=sn, fullfetch=fullfetch)
    
def checkProcesses(processes, pidFile):
    rem=[]
    p = len(processes)
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
                    log.info("ABORT", PID= process.pid, portalID=portalID, apiurl=apiurl, start=start.isoformat(), exitcode=process.exitcode)
                    pidFile.write("ABORT\t %s \t %s \t %s \t %s (%s)\n"%(process.pid,process.exitcode,end, portalID, apiurl))
                pidFile.flush()
            except Exception as e:
                print e
    for pID in rem:
        del processes[pID]
    assert p-len(processes) == len(rem) 
    return len(rem)

def name():
    return 'Fetch'
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
        p = dbm.getPortal( apiurl=args.url)
        pmd = dbm.getPortalMetaData(portalID=p.id, snapshot=sn)
        if not pmd:
            pmd = PortalMetaData(portalID=p.id, snapshot=sn)
            dbm.insertPortalMetaData(pmd)
            
        log.info("Queuing", pid=p.id, apiurl=args.url)
        jobs.append( {'portal':p, 'sn':sn, 'dbm':dbm, 'fullfetch':fetch } )
    else:
        for p in Portal.iter(dbm.getPortals()):
            pmd = dbm.getPortalMetaData(portalID=p.id, snapshot=sn)
            if not pmd:
                pmd = PortalMetaData(portalID=p.id, snapshot=sn)
                dbm.insertPortalMetaData(pmd)
                
            log.info("Queuing", pid=p.id)
            jobs.append( {'portal':p, 'sn':sn, 'dbm':dbm, 'fullfetch':fetch } )        
    try:
        log.info("Start processing", portals=len(jobs), processors=args.processors, start = time.time())
        
        #headProcess = HeadProcess(dbm, sn)
        #headProcess.start()
        
        processes={}
        
        fetch_processors = args.processors
        
        checks=0
        p_done=0
        with args.outfile as pidFile:
            pidFile.write("STATUS\t PID \t start \t p_id \t p_url\n")
            pidFile.flush()
            
            total=len(jobs)
            c=0
            start = time.time()
            for job in jobs:
                p = Process(target=fetching, args=((job,)))
                p.start()
                c+=1
            
                p_start = datetime.now()
                processes[job['portal'].id] = (p.pid, p, p_start, job['portal'].apiurl)
                
                log.info("START", processID= p.pid, pid=job['portal'].id, apiurl=job['portal'].apiurl, start=p_start)
                pidFile.write("START\t %s  \t %s \t %s (%s)\n"%(p.pid, p_start, job['portal'].id, job['portal'].apiurl))
                pidFile.flush()
                
                while len(processes) >= fetch_processors:
                    p_done += checkProcesses(processes, pidFile)
                    checks+=1
                    if checks % 90==0:
                        log.info("StatusCheck", checks=checks, runningProcsses=len(processes), done=(c-len(processes)), remaining=(total-c))
                    time.sleep(10)
                elapsed = (time.time() - start)
                util.progressIndicator(p_done, total, elapsed=elapsed,label='Portals')
        while len(processes)>0:
            p_done += checkProcesses(processes, pidFile)
            checks+=1
            if checks % 90==0:
                log.info("StatusCheck", checks=checks, runningProcsses=len(processes), done=(c-len(processes)), remaining=(total-c))
            time.sleep(10)
            
            elapsed = (time.time() - start)
            util.progressIndicator(p_done, total, elapsed=elapsed,label='Portals')

        #headProcess.shutdown()        
        #headProcess.join()
        
        #log.info("RestartHeadLookups")
        #headProcess = HeadProcess(dbm, sn)
        #headProcess.start()
        #headProcess.shutdown()
        #headProcess.join()
    except Exception as e:
        eh.handleError(log, "ProcessingFetchException", exception=e, exc_info=True) 