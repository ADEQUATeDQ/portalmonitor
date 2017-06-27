import datetime
import json
import os
from multiprocessing import Pool

import rdflib
import structlog
import yaml
from scrapy.utils.url import escape_ajax
from sqlalchemy import not_
from w3lib.url import safe_url_string

from odpw.utils import dynamicity

log =structlog.get_logger()

import urlnorm

from odpw.quality.dcat_analysers import dcat_analyser
from odpw.utils.utils_snapshot import getCurrentSnapshot

from odpw.services.aggregates import aggregatePortalQuality

from odpw.utils.helper_functions import readDBConfFromFile, md5
from odpw.utils.error_handling import ErrorHandler, getExceptionCode, getExceptionString
from odpw.utils.timing import Timer

from odpw.core.model import Dataset, DatasetData, PortalSnapshot, MetaResource, \
    DatasetQuality, Portal, PortalSnapshotQuality
from odpw.core.dataset_converter import dict_to_dcat
from odpw.core.dcat_access import getDistributionAccessURLs, getDistributionDownloadURLs, getDistributionFormatWithURL, \
    getDistributionMediaTypeWithURL, getDistributionSizeWithURL, getDistributionCreationDateWithURL, \
    getDistributionModificationDateWithURL, getOrganization, getCreationDate, getModificationDate, getLicense, getTitle
from odpw.core.parsing import toDatetime, normaliseFormat
from odpw.core.portal_fetch_processors import getPortalProcessor
from odpw.core.db import  DBManager
from odpw.core.api import DBClient

from random import randint
from time import sleep


def fetchHttp(obj):
    P, dbConf, snapshot, store_local = obj[0],obj[1],obj[2],obj[3]
    log.info("HTTPInsert", portalid=P.id, snapshot=snapshot)

    dbm = DBManager(**dbConf)
    db = DBClient(dbm)

    with Timer(key='InsertPortal', verbose=True):
        PS= PortalSnapshot(portalid=P.id, snapshot=snapshot)
        PS.start=datetime.datetime.now()
        sleep(randint(1, 10))
        db.add(PS)
        try:

            processor=getPortalProcessor(P)
            iter=processor.generateFetchDatasetIter(P, PS, snapshot)
            insertDatasets(P,db,iter,snapshot, store_local=store_local)
            status=200
            exc=None
            db.commit()
        except Exception as exc:
            ErrorHandler.handleError(log, "PortalFetchException", exception=exc, pid=P.id, snapshot=snapshot, exc_info=True)
            status=getExceptionCode(exc)
            exc=getExceptionString(exc)
        try:
            #update the portalsnapshot object with dataset and resource count and end time
            dsCount=PS.datasetcount
            dsfetched=db.Session.query(Dataset).filter(Dataset.snapshot==snapshot).filter(Dataset.portalid==P.id).count()
            resCount=db.Session.query(Dataset).filter(Dataset.snapshot==snapshot).filter(Dataset.portalid==P.id).join(MetaResource,MetaResource.md5==Dataset.md5).count()

        except Exception as exc:
            ErrorHandler.handleError(log, "PortalSnapshotUpdate", exception=exc, pid=P.id, snapshot=snapshot,
                                     exc_info=True)
        try:
            s = db.Session

            PS = s.query(PortalSnapshot).filter(PortalSnapshot.portalid==P.id, PortalSnapshot.snapshot==snapshot).first()
            PS.datasetsfetched =dsfetched
            PS.resourcecount =resCount
            PS.datasetcount = dsCount
            PS.end = datetime.datetime.now()
            PS.exc=exc
            PS.status=status
            s.commit()
            #s.flush()
            s.remove()
        except Exception as exc:
            ErrorHandler.handleError(log, "PortalSnapshotUpdate", exception=exc, pid=P.id, snapshot=snapshot, exc_info=True)
        try:
            aggregatePortalQuality(db,P.id, snapshot)
        except Exception as exc:
            ErrorHandler.handleError(log, "PortalFetchAggregate", exception=exc, pid=P.id, snapshot=snapshot, exc_info=True)

        # compute dynamicity stats
        try:
            sn = [ps.snapshot for ps in db.Session.query(PortalSnapshot).filter(PortalSnapshot.portalid == P.id)]
            sn=sorted(sn)
            sn_i = sn.index(snapshot)
            dynamicity.dynPortal(db, P, snapshot, sn[sn_i-1])
        except Exception as exc:
            ErrorHandler.handleError(log, "PortalDynamicity", exception=exc, pid=P.id, snapshot=snapshot,
                                     exc_info=True)

    return (P, snapshot)

def createDatasetData(md5v,dataset):
    with Timer(key='createDatasetData'):
        DD= DatasetData(md5=md5v, raw=dataset.data)

        cd=getCreationDate(dataset)
        md=getModificationDate(dataset)

        DD.modified     = toDatetime(cd[0] if len(cd)>0 else None)
        DD.created      = toDatetime(md[0] if len(md)>0 else None)
        DD.organisation = getOrganization(dataset)
        DD.license      = getLicense(dataset)
        return DD

def createDatasetQuality(P, md5v, dataset):
    with Timer(key='quality'):

        q={}
        for id,qa in  dcat_analyser().items():
            q[qa.id.lower()]=qa.analyse_Dataset(dataset)

        DQ=DatasetQuality(md5=md5v, **q)
        return DQ

def createMetaResources(md5v,dataset):
    with Timer(key='createMetaResources'):
        res= getDistributionAccessURLs(dataset)+getDistributionDownloadURLs(dataset)
        bulk_mr=[]
        uris=[]
        for uri in res:
            valid=True
            try:
                uri = urlnorm.norm(uri.strip())
            except Exception as e:
                log.debug("URIFormat", uri=uri, md5=md5v, msg=e.message)
                uri=uri
                valid=False

            f=getDistributionFormatWithURL(dataset, uri)
            m=getDistributionMediaTypeWithURL(dataset, uri)
            s=getDistributionSizeWithURL(dataset, uri)
            c=getDistributionCreationDateWithURL(dataset,uri)
            mod=getDistributionModificationDateWithURL(dataset,uri)
            try:
                s_uri = safe_url_string(uri, 'utf-8')
                uri = escape_ajax(s_uri)
            except Exception as exc:
                ErrorHandler.handleError(log, "safe_url_string", exception=exc, md5=md5, uri=uri,
                                         exc_info=True)
                uri = uri

            if uri in uris:
                log.debug("WARNING, duplicate URI", dataset=dataset.id, md5=md5v, uri=uri,format=f,media=m)
                continue
            try:
                s=int(float(s)) if s is not None else None
            except Exception as e:
                s=None



            MR= MetaResource(
                uri = uri
                ,md5 = md5v
                ,media=m
                ,valid=valid
                ,format = normaliseFormat(f)
                ,size = s
                ,created = toDatetime(c)
                ,modified=toDatetime(mod)
            )
            bulk_mr.append(MR)
            uris.append(uri)
        return bulk_mr

def bulkInsert(bulk_obj, db ):
    with Timer(key='bulkInsert'):
        for k in ['dq','mr','d']:
            db.bulkadd(bulk_obj[k])
            bulk_obj[k]=[]


def insertDatasets(P, db, iter, snapshot, batch=100, store_local=False):

    log.info("insertDatasets", portalid=P.id ,snapshot=snapshot)

    bulk_obj={ 'mr':[]
               ,'d':[]
               ,'dq':[]}

    c=0
    for i, d in enumerate(iter):
        c+=1
        with Timer(key='ProcessDataset'):
            #CREATE DATASET AND ADD


            with Timer(key='md5'):
                md5v=None if d.data is None else md5(d.data)


            if md5v:
                with Timer(key='dict_to_dcat'):
                    #analys quality
                    d.dcat=dict_to_dcat(d.data, P)

                # store metadata in local git directory
                if store_local != None:
                    with Timer(key='store_to_local_git'):
                        if 'name' in d.data:
                            dir_name = d.data['name']
                        else:
                            dir_name = d.id
                        filename = os.path.join(store_local, P.id, dir_name)
                        if not os.path.exists(filename):
                            os.makedirs(filename)
                        with open(os.path.join(filename, 'metadata.json'), 'w') as f:
                            json.dump(d.data, f, indent=4)
                        with open(os.path.join(filename, 'dcat_metadata.ttl'), 'w') as f:
                            g = rdflib.Graph()
                            g.parse(data=json.dumps(d.dcat), format='json-ld')
                            g.serialize(f, format='ttl')

                DD=None
                with Timer(key='db.datasetdataExists(md5v)'):
                    process = not db.exist_datasetdata(md5v)
                if process:
                    #DATATSET DATA
                    DD=createDatasetData(md5v,d)
                    try:
                        db.add(DD) #primary key, needs to be inserted first
                        #DATATSET QUALTIY
                        #print "adding",md5v
                        DQ = createDatasetQuality(P, md5v, d)
                        bulk_obj['dq'].append(DQ)

                        #META RESOURCES
                        MQs= createMetaResources(md5v , d)
                        for MR in MQs:
                            bulk_obj['mr'].append(MR)
                    except Exception as e:
                        pass
                        #print "AND AGAIN",md5v, db.datasetdataExists(md5v)
                #DATATSET
                title=getTitle(d)
                title = title[0] if len(title)>0 else None

                D= Dataset(id=d.id,
                       snapshot=d.snapshot,
                       portalid=d.portal_id,
                       md5=md5v,
                       organisation=DD.organisation if DD else getOrganization(d),
                       title=title
                        )

                bulk_obj['d'].append(D)
            else:
                D= Dataset(id=d.id,
                       snapshot=d.snapshot,
                       portalid=d.portal_id,
                       md5=md5v,
                       organisation=None)
                bulk_obj['d'].append(D)

        if i%batch==0:
            bulkInsert(bulk_obj,db )
            for k in bulk_obj:
                bulk_obj[k]=[]
        c=i

    #cleanup, commit all left inserts
    bulkInsert(bulk_obj,db)
    for k in bulk_obj:
        bulk_obj[k]=[]
    log.info("InsertedDatasets", parsed=c, portalid=P.id ,snapshot=snapshot)


#--*--*--*--*
def help():
    return "perform head lookups"
def name():
    return 'Fetch'

def setupCLI(pa):
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=4)
    pa.add_argument('--pid', dest='portalid' , help="Specific portal id ")
    pa.add_argument('--repair', help="Re-fetch portals with status != 200", action='store_true')

def cli(args,dbm):
    sn = getCurrentSnapshot()

    dbConf= readDBConfFromFile(args.config)
    db= DBClient(dbm)

    store_local = None
    if args.config:
        with open(args.config) as f:
            config = yaml.load(f)
            if 'git' in config and 'datadir' in config['git']:
                store_local = config['git']['datadir']

    tasks=[]
    if args.portalid:
        P =db.Session.query(Portal).filter(Portal.id==args.portalid).one()
        if P is None:
            log.warn("PORTAL NOT IN DB", portalid=args.portalid)
            return
        else:
            tasks.append((P, dbConf, sn, store_local))
    else:
        if args.repair:
            valid = db.Session.query(PortalSnapshot.portalid).filter(PortalSnapshot.snapshot==sn).filter(PortalSnapshot.status==200).subquery()

            for P in db.Session.query(Portal).filter(Portal.id.notin_(valid)):
                PS=db.Session.query(PortalSnapshot).filter(PortalSnapshot.snapshot==sn).filter(PortalSnapshot.portalid==P.id)
                db.delete(PS)
                PSQ = db.Session.query(PortalSnapshotQuality).filter(PortalSnapshotQuality.snapshot == sn).filter(
                    PortalSnapshotQuality.portalid == P.id)
                db.delete(PSQ)

                tasks.append((P, dbConf, sn, store_local))
        else:
            for P in db.Session.query(Portal):
                tasks.append((P, dbConf, sn, store_local))

    log.info("START FETCH", processors=args.processors, dbConf=dbConf, portals=len(tasks))

    pool = Pool(args.processors)
    for x in pool.imap(fetchHttp,tasks):
        pid,sn =x[0].id, x[1]
        log.info("RECEIVED RESULT", portalid=pid, snapshot=sn)
