from datetime import datetime
from multiprocessing.process import Process
from time import sleep
from odpw.head import HeadProcess
__author__ = 'jumbrich'

from db.models import Portal, Dataset, PortalMetaData, Resource

import ckanclient
import util
from util import getSnapshot,getExceptionCode,ErrorHandler as eh

from timer import Timer
import math
import argparse

import logging
logger = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()

import requests 

from random import randint
import time


import json
import hashlib

def fetchAllDatasets(package_list, stats, dbm, sn, fullfetch):
    stats['res']=[]
    Portal = stats['portal']
    c=0
    
    # get all meta data as json 
    for datasetJSON in ckanclient.full_metadata_list(stats['portal'].apiurl):
        datasetID = datasetJSON['name']
        data = datasetJSON['data']
        try:
            props= analyseDataset(data, datasetID,stats, dbm, sn, 200)

            #remove dataset form package_list
            package_list.remove(datasetID)
        
            d = Dataset(snapshot=sn,portal=stats['portal'].id, dataset=datasetID, **props)
            dbm.insertDatasetFetch(d)
        
            c = c + 1
            if (c > 0) and (math.fmod(c, 100) == 0):
                log.info('process status', pid=Portal.id, done=c, total=Portal.datasets)
        except Exception as e:
            eh.handleError(log, "GET MetaData", exception=e, pid=Portal.id,  did=datasetID,exc_info=True)
    
    #process remaining datasets which were not available in the fullMetaData list    
    for entity in package_list:
        #WAIT between two consecutive GET requests
        time.sleep(randint(1, 2))
        try:
            log.debug("GET MetaData", pid=Portal.id, did=entity)

            with Timer(key="fetchDS("+Portal.id+")") as t, Timer(key="fetchDS") as t1:
                #fetchDataset(entity, stats, dbm, sn)
                props={
                        'status':-1,
                        'md5':None,
                        'data':None,
                        'exception':None
                        }
                try:
                    resp = ckanclient.package_entity(stats['portal'].apiurl, entity)
                    props=analyseDataset(resp.json(), entity, stats, dbm, sn,resp.status_code)
                    
                except Exception as e:
                    eh.handleError(log,'fetching dataset information', exception=e,pid=stats['portal'].id,
                                   apiurl=stats['portal'].apiurl,
                                   exc_info=True)
                    props['status']=util.getExceptionCode(e)
                    props['exception']=str(type(e))+":"+str(e.message)
                
                d = Dataset(snapshot=sn,portal=stats['portal'].id, dataset=entity, **props)
                dbm.insertDatasetFetch(d)
            c = c + 1
            if (c > 0) and (math.fmod(c, 100) == 0):
                log.info('process status', pid=Portal.id, done=c, total=Portal.datasets)

        except Exception as e:
            eh.handleError(log, "GET MetaData", exception=e, pid=Portal.id,  did=entity,exc_info=True)
            
    log.info("Fetched Meta data", pid=Portal.id, done=c, total=len(package_list))

def analyseDataset(entityJSON, datasetID,  stats, dbm, sn, status,first=False):
    props={
        'status':-1,
        'md5':None,
        'data':None,
        'exception':None
        }
    try:
        #resp = ckanclient.package_entity(stats['portal'].apiurl, entity)

        props['status']=status

        cnt= stats['fetch_stats']['respCodes'].get(status,0)
        stats['fetch_stats']['respCodes'][status]= (cnt+1)

        if status == requests.codes.ok:
            data = entityJSON
            
            d = json.dumps(data, sort_keys=True, ensure_ascii=True)
            data_md5 = hashlib.md5(d).hexdigest()
            props['md5']=data_md5
            
            props['data']=data

            stats=extract_keys(data, stats)

            
            if 'resources' in data:
                stats['res'].append(len(data['resources']))
                lastDomain=None
                for resJson in data['resources']:
                    stats['res_stats']['total']+=1

                    tR =  Resource.newInstance(url=resJson['url'], snapshot=sn)
                    R = dbm.getResource(tR)
                    if not R:
                        #do the lookup
                        with Timer(key="newRes") as t:
                            R = Resource.newInstance(url=resJson['url'], snapshot=sn)
                            dbm.insertResource(R)
                        
                    R.updateOrigin(pid=stats['portal'].id, did=datasetID)
                    dbm.updateResource(R)
                    
    except Exception as e:
        print e
        eh.handleError(log,'fetching dataset information', exception=e,pid=stats['portal'].id,
                  apiurl=stats['portal'].apiurl,
                  exc_info=True)
        props['status']=util.getExceptionCode(e)
        props['exception']=str(type(e))+":"+str(e.message)

    return props

def extract_keys(data, stats):

    core=stats['general_stats']['keys']['core']
    extra=stats['general_stats']['keys']['extra']
    res=stats['general_stats']['keys']['res']

    for key in data.keys():
        if key == 'resources':
            for r in data['resources']:
                for k in r.keys():
                    if k not in res:
                        res.append(k)
        elif key == 'extras' and isinstance(data['extras'],dict):
            for k in data['extras'].keys():
                if k not in extra:
                    extra.append(k)
        else:
            if key not in core:
                core.append(key)

    return stats

def fetchingDummy(obj):
    sleep(10)

def fetching(obj):
    Portal = obj['portal']
    sn=obj['sn']
    dbm=obj['dbm']
    fullfetch=obj['fullfetch']

    log.info("Fetching", pid=Portal.id, sn=sn, fullfetch=fullfetch)

    stats={
        'portal':Portal,
        'datasets':-1, 'resources':-1,
        'status':-1,
        'fetch_stats':{'respCodes':{}, 'fullfetch':fullfetch},
        'general_stats':{
            'datasets':0,
            'keys':{'core':[],'extra':[],'res':[]}
        },
        'res_stats':{'respCodes':{},'total':0, 'resList':[]}
    }
    
    pmd = dbm.getPortalMetaData(portalID=Portal.id, snapshot=sn)
    if not pmd:
        pmd = PortalMetaData(portal=Portal.id, snapshot=sn)
        dbm.insertPortalMetaData(pmd)
    else:
        pmd.fetchstart()
        dbm.updatePortalMetaData(pmd)
    
    try:
        if fullfetch:
            #fetch the dataset descriptions
            resp = ckanclient.package_get(Portal.apiurl)
            stats['status']=resp.status_code
            Portal.status=resp.status_code

            if resp.status_code != requests.codes.ok:
                log.error("No package list received", apiurl=Portal.apiurl, status=resp.status_code)
            else:
                package_list = resp.json()
                Portal.datasets=len(package_list)

                log.info('Received packages', apiurl=Portal.apiurl, status=resp.status_code, count=Portal.datasets)

                stats['datasets']=Portal.datasets

                fetchAllDatasets(package_list, stats, dbm, sn, fullfetch)

                Portal.resources=sum(stats['res'])
                Portal.latest_snapshot=sn
                stats['resources']=Portal.resources

    except Exception as e:
        eh.handleError(log,"fetching dataset information", exception=e, apiurl=Portal.apiurl,exc_info=True)
        #log.exception('fetching dataset information', apiurl=Portal.apiurl,  exc_info=True)
        Portal.status=getExceptionCode(e)
        Portal.exception=str(type(e))+":"+str(e.message)
    try:
        pmd.updateStats(stats)
        ##UPDATE
        #General portal information (ds, res)
        dbm.updatePortal(Portal)
        #Portal Meta Data
        #   ds-fetch statistics
        dbm.updatePortalMetaData(pmd)
    except Exception as e:
        eh.handleError(log,'Updating DB',exception=e,pid=Portal.id, exc_info=True)
        log.critical('Updating DB', pid=Portal.id, exctype=type(e), excmsg=e.message,exc_info=True)

    return stats


def checkProcesses(processes, pidFile, job):
    for portalID in processes.keys():
        (pid, process, start) = processes[portalID]
                        
        if not process.is_alive():
            process.join() # Allow tidyup
            status = process.exitcode
            end = datetime.now()
            if status ==0:
                log.info("FINISHED", PID= process.pid, portalID=job['portal'].id, apiurl=job['portal'].apiurl, start=start, exitcode=process.exitcode)
                pidFile.write("FINISHED\t %s \t %s (%s)\t %s \t %s \n"%(process.pid, job['portal'].id, job['portal'].apiurl,process.exitcode,end))
            else:
                log.info("ABORTED", PID= process.pid, portalID=job['portal'].id, apiurl=job['portal'].apiurl, start=start, exitcode=process.exitcode)
                pidFile.write("ABORTED\t %s \t %s (%s)\t %s \t %s \n"%(process.pid, job['portal'].id, job['portal'].apiurl,process.exitcode,end))
            pidFile.flush()
            del processes[portalID] # Removed finished items from the dictionary

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
        
        for portalRes in dbm.getUnprocessedPortals(snapshot=sn):
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
        with args.outfile as pidFile:
            
            total=len(jobs)
            c=0
            for job in jobs:
                p = Process(target=fetching, args=((job,)))
                p.start()
                c+=1
            
                start = datetime.now()
                processes[job['portal'].id]=(p.pid, p, start )
                
                log.info("STARTED", PID= p.pid, portalID=job['portal'].id, apiurl=job['portal'].apiurl, start=start, datasets=job['portal'].datasets)
                pidFile.write("STARTED\t %s \t %s (%s)\t %s \t %s \n"%(p.pid, job['portal'].id, job['portal'].apiurl,job['portal'].datasets,start))
                pidFile.flush()
                
                while len(processes) >= fetch_processors:
                
                    checkProcesses(processes,pidFile,job)
                    
                    sleep(1)
                
                util.progressINdicator(c, total)
            
            while len(processes) >0 :
                checkProcesses(processes,pidFile,job)
                sleep(1)
        
        headProcess.stop()        
        headProcess.join()
        headProcess = HeadProcess(dbm, sn)
        headProcess.start()
        headProcess.stop()
        headProcess.join()
        
    except Exception as e:
        eh.handleError(log, "Processing fetch", exception=e, exc_info=True)

    


    #dbm.updateTimeInSnapshotStatusTable(sn=sn, key="fetch_end")