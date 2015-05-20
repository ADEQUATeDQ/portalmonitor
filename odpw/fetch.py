__author__ = 'jumbrich'

from db.POSTGRESManager import PostGRESManager
from db.models import Portal
from db.models import Dataset
from db.models import PortalMetaData
from db.models import Resource
import ckanclient
import util
from util import getExceptionCode
from util import getSnapshot

from timer import Timer
import sys
import math

import logging
logger = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()

from datetime import datetime

import requests
from random import randint
import time

from multiprocessing import Pool, Lock, Value
from multiprocessing.dummy import Pool as ThreadPool
import json
import hashlib

def fetchAllDatasets(package_list, stats, dbm, sn, fullfetch):
    stats['fetch_stats']['dslist']={ 'total':[], 'success':[], 'server':[],'failed':[]}
    stats['res']=[]
    Portal = stats['portal']
    c=0
    for entity in package_list:
        stats['fetch_stats']['dslist']['total'].append(entity)
        #WAIT between two consecutive GET requests
        time.sleep(randint(1, 2))

        try:
            log.debug("GET MetaData", pid=Portal.id, did=entity)

            with Timer(key="fetchDS("+Portal.id+")") as t, Timer(key="fetchDS") as t1:
                fetchDataset(entity, stats, dbm, sn)

            c = c + 1
            if (c > 0) and (math.fmod(c, 100) == 0):
                log.debug('process status', pid=Portal.id, done=c, total=Portal.datasets)

        except Exception as e:
            log.warning("GET MetaData", pid=Portal.id, exctype=type(e), excmsg=e.message, did=entity)
            log.exception('GET MetaData', pid=Portal.id,  did=entity,exc_info=True)
    log.info("Fetched Meta data", pid=Portal.id, done=c, total=len(package_list))

def fetchDataset(entity, stats, dbm, sn, first=False):
    resp = ckanclient.package_entity(stats['portal'].apiurl, entity)

    props={
        'status':resp.status_code,
        'md5':None,
        'data':{}
        }

    if 500 <= resp.status_code < 600:
        # server error, we can try it again
        stats['fetch_stats']['dslist']['server'].append(entity)
        if not first:
            cnt = stats['fetch_stats']['respCodes'].get(resp.status_code,0)
            stats['fetch_stats']['respCodes'][resp.status_code]= (cnt+1)

            d = Dataset(snapshot=sn,portal=stats['portal'].id,dataset=entity, **props)
            dbm.upsertDatasetFetch(d)
    else:
        cnt= stats['fetch_stats']['respCodes'].get(resp.status_code,0)
        stats['fetch_stats']['respCodes'][resp.status_code]= (cnt+1)

        if resp.status_code == requests.codes.ok:
            data = resp.json()
            stats['fetch_stats']['dslist']['success'].append(entity)
            d = json.dumps(data, sort_keys=True, ensure_ascii=True)
            data_md5 = hashlib.md5(d).hexdigest()
            props['md5']=data_md5
            props['data']=data

            stats=extract_keys(data, stats)

            if 'resources' in data:
                stats['res'].append(len(data['resources']))
                for resJson in data['resources']:

                    R = dbm.getResource(url=resJson['url'], snapshot=sn)
                    if not R:
                    #do the lookup
                        with Timer(key="newRes") as t:
                            R = Resource.newInstance(url=resJson['url'], snapshot=sn)
                    R.updateOrigin(pid=stats['portal'].id, did=entity)
                    dbm.upsertResource(R)


        else:
            stats['fetch_stats']['dslist']['failed'].append(entity)

        d = Dataset(snapshot=sn,portal=stats['portal'].id,dataset=entity, **props)
        dbm.upsertDatasetFetch(d)

def extract_keys(data, stats):

    core=stats['general_stats']['keys']['core']
    extra=stats['general_stats']['keys']['extra']
    res=stats['general_stats']['keys']['res']

    for key in data.keys():
        if key == 'resources':
            for r in data['resources']:
                for key in r.keys():
                    if key not in res:
                        res.append(key)
        elif key == 'extras':
            for key in data['extras'].keys():
                if key not in extra:
                    extra.append(key)
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
        }
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

                log.info('Received packages', apiurl=Portal.apiurl, status=resp.status_code)

                stats['datasets']=Portal.datasets

                fetchAllDatasets(package_list, stats, dbm, sn, fullfetch)

                Portal.resources=sum(stats['res'])
                Portal.latest_snapshot=sn
                stats['resources']=Portal.resources

    except Exception as e:
        log.warning("fetching dataset information", apiurl=Portal.apiurl, exctype=type(e), excmsg=e.message)
        log.exception('fetching dataset information', apiurl=Portal.apiurl,  exc_info=True)
        Portal.status=getExceptionCode(e)
    try:
        pmd.updateFetchStats(stats)
        ##UPDATE
        #General portal information (ds, res)
        dbm.upsertPortal(Portal)
        #Portal Meta Data
        #   ds-fetch statistics
        dbm.upsertPortalMetaData(pmd)
    except Exception as e:
        log.error('Updating DB', pid=Portal.id, exctype=type(e), excmsg=e.message)
        log.exception('Updating DB', pid=Portal.id,  exc_info=True)

    return stats





def name():
    return 'Fetch'

def setupCLI(pa):
    gfilter = pa.add_argument_group('filters', 'filter option')
    gfilter.add_argument('-d','--datasets',type=int, dest='ds', help='filter portals with more than specified datasets')
    gfilter.add_argument('-r','--resources',type=int, dest='res')
    gfilter.add_argument('-s','--software',choices=['CKAN'], dest='software')
    gfilter.add_argument('-u','--url',type=str, dest='url')

    pa.add_argument("--force", action='store_true', help='force a full fetch, otherwise use update',dest='fetch')
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument("-p","--procs", type=int, help='Number of processors to use', dest='processors', default=1)

def cli(args,dbm):

    sn = getSnapshot(args)
    if not sn:
        return

    jobs=[]
    fetch=True
    if args.fetch:
        fetch=True
    if args.url:
        p= dbm.getPortal(url=args.url)
        log.info("Queuing", pid=p.id, datasets=p.datasets, resources=p.resources)
        jobs.append(
            {   'portal':p,
                'sn':sn,
                'dbm':dbm,
                'fullfetch':fetch
            }
        )
    else:
        for result in dbm.getPortals(maxDS=args.ds, maxRes=args.res, software=args.software,status=200):
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

        log.info("fetch result", total=portals['count'], active=portals['ok'], offline=portals['offline'], servererror=portals['serverErr'], connectionerror=portals['connErr'])

    except Exception as e:
        log.exception(e)

    #Timer.printStats()
    Timer.getStats()

    #dbm.updateTimeInSnapshotStatusTable(sn=sn, key="fetch_end")