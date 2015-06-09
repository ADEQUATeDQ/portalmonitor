__author__ = 'jumbrich'

from db.models import Portal
from db.models import Dataset
from db.models import PortalMetaData
from db.models import Resource
import ckanclient
import util
from util import getExceptionCode
from util import getSnapshot

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

from multiprocessing.dummy import Pool as ThreadPool
import json
import hashlib

def fetchAllDatasets(package_list, stats, dbm, sn, fullfetch):
    stats['res']=[]
    Portal = stats['portal']
    c=0
    for entity in package_list:

        #WAIT between two consecutive GET requests
        time.sleep(randint(1, 2))

        try:
            log.debug("GET MetaData", pid=Portal.id, did=entity)

            with Timer(key="fetchDS("+Portal.id+")") as t, Timer(key="fetchDS") as t1:
                fetchDataset(entity, stats, dbm, sn)

            c = c + 1
            if (c > 0) and (math.fmod(c, 100) == 0):
                log.info('process status', pid=Portal.id, done=c, total=Portal.datasets)

        except Exception as e:
            log.error("GET MetaData", pid=Portal.id, exctype=type(e), excmsg=e.message, did=entity,exc_info=True)
            #log.exception('GET MetaData', pid=Portal.id,  did=entity,exc_info=True)
    log.info("Fetched Meta data", pid=Portal.id, done=c, total=len(package_list))

def fetchDataset(entity, stats, dbm, sn, first=False):
    props={
        'status':-1,
        'md5':None,
        'data':None,
        'exception':None
        }
    try:
        resp = ckanclient.package_entity(stats['portal'].apiurl, entity)

        props['status']=resp.status_code

        cnt= stats['fetch_stats']['respCodes'].get(resp.status_code,0)
        stats['fetch_stats']['respCodes'][resp.status_code]= (cnt+1)

        if resp.status_code == requests.codes.ok:
            data = resp.json()
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

                    R = dbm.getResource(url=resJson['url'], snapshot=sn)
                    if not R:
                        #do the lookup
                        with Timer(key="newRes") as t:
                            R = Resource.newInstance(url=resJson['url'], snapshot=sn)
                        
                        curdomain=util.computeID(R.url)
                        if lastDomain and curdomain and lastDomain==curdomain:
                            #WAIT between two consecutive GET requests
                            time.sleep(randint(1, 2))
                        lastDomain=curdomain

                    R.updateOrigin(pid=stats['portal'].id, did=entity)
                    dbm.upsertResource(R)

                    cnt= stats['res_stats']['respCodes'].get(R.status,0)
                    stats['res_stats']['respCodes'][R.status]= (cnt+1)

                    if R.url not in stats['res_stats']['resList']:
                        stats['res_stats']['resList'].append(R.url)


    except Exception as e:
        log.error('fetching dataset information', pid=stats['portal'].id,apiurl=stats['portal'].apiurl,exctype=type(e), excmsg=e.message,exc_info=True)
        props['status']=util.getExceptionCode(e)
        props['exception']=str(type(e))+":"+str(e.message)

    d = Dataset(snapshot=sn,portal=stats['portal'].id,dataset=entity, **props)
    dbm.upsertDatasetFetch(d)

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
            'keys':{'core':[],'extra':[],'res':[]}
        },
        'res_stats':{'respCodes':{},'total':0, 'resList':[]}
    }
    pmd = PortalMetaData(portal=Portal.id, snapshot=sn)

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
        log.error("fetching dataset information", apiurl=Portal.apiurl, exctype=type(e), excmsg=e.message,exc_info=True)
        #log.exception('fetching dataset information', apiurl=Portal.apiurl,  exc_info=True)
        Portal.status=getExceptionCode(e)
        Portal.exception=str(type(e))+":"+str(e.message)
    try:
        pmd.updateFetchStats(stats)
        ##UPDATE
        #General portal information (ds, res)
        dbm.upsertPortal(Portal)
        #Portal Meta Data
        #   ds-fetch statistics
        dbm.upsertPortalMetaData(pmd)
    except Exception as e:
        log.critical('Updating DB', pid=Portal.id, exctype=type(e), excmsg=e.message,exc_info=True)
        #log.exception('Updating DB', pid=Portal.id,  exc_info=True)

    return stats





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
        
        sql="SELECT p.* FROM portals p WHERE p.id NOT IN ( select pmd.portal from portal_meta_data pmd WHERE snapshot='"+sn+"' AND p.id=pmd.portal) ORDER  BY p.datasets;"
        with args.outfile as file:
            for pRes in dbm.selectQuery(sql):
                file.write(pRes['apiurl']+"\n")
        
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
        for result in dbm.getPortals(maxDS=args.ds, maxRes=args.res, software=args.software):
            p = Portal.fromResult(result)
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
        pool = ThreadPool(processes=args.processors)
        results = pool.map(fetching, jobs)
        pool.close()
        pool.join()

        portals=util.initStatusMap()
        for r in results:
            log.info("Done", pid=r['portal'].id, status=r['status'])
            util.analyseStatus(portals,r['status'])

        log.info("fetch result", data=portals)

    except Exception as e:
        log.error("Processing fetch",exctype=type(e), excmsg=e.message,exc_info=True)

    Timer.printStats()
    log.info("Timer", stats=Timer.getStats())



    #dbm.updateTimeInSnapshotStatusTable(sn=sn, key="fetch_end")