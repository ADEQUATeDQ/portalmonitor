import datetime
import json
from multiprocessing import Pool

import rdflib
import structlog

from db.dbm import PostgressDBM
from odpw.new.quality import dcat_analyser
from odpw.new.utils.utils_snapshot import getCurrentSnapshot

log =structlog.get_logger()
import urlnorm


from odpw.new.services.aggregates import aggregatePortalQuality

from odpw.new.utils.helper_functions import readDBConfFromFile, md5
from odpw.new.utils.error_handling import ErrorHandler, getExceptionCode, getExceptionString
from odpw.new.utils.timing import Timer

from odpw.new.core.model import Dataset, DatasetData, PortalSnapshot, MetaResource, \
    DatasetQuality, Portal
from odpw.new.core.dataset_converter import dict_to_dcat
from odpw.new.core.dcat_access import getDistributionAccessURLs, getDistributionDownloadURLs, getDistributionFormatWithURL, \
    getDistributionMediaTypeWithURL, getDistributionSizeWithURL, getDistributionCreationDateWithURL, \
    getDistributionModificationDateWithURL, getOrganization, getCreationDate, getModificationDate, getLicense
from odpw.new.core.parsing import toDatetime, normaliseFormat
from odpw.new.core.portal_fetch_processors import getPortalProcessor
from odpw.new.core.db import DBClient, DBManager



def fetchMigrate(obj):

    P, dbConf, snapshot = obj[0],obj[1],obj[2]

    dbm = DBManager(**dbConf)
    db= DBClient(dbm)


    dbm1=PostgressDBM(user='opwu', password='0pwu', host='portalwatch.ai.wu.ac.at', port=5432, db='portalwatch')
    PMD= dbm.getPortalMetaData(portalID=P.id, snapshot=snapshot)
    if PMD is None:
        log.info("Skipping ",portalid=P.id, snapshot=snapshot)
        return
    PS= PortalSnapshot(portalid=P.id, snapshot=snapshot)

    if PMD.fetch_stats is not None and 'fetch_start' in PMD.fetch_stats:

        start= datetime.datetime.strptime(PMD.fetch_stats['fetch_start'], "%Y-%m-%dT%H:%M:%S.%f")
        end= datetime.datetime.strptime(PMD.fetch_stats['fetch_end'], "%Y-%m-%dT%H:%M:%S.%f") if 'fetch_end' in PMD.fetch_stats and PMD.fetch_stats['fetch_end'] is not None else None
        PS.start=start
        PS.end=end
        PS.exc=PMD.fetch_stats['exception']
        PS.status=PMD.fetch_stats['status']
    else:
        PS.start=None
        PS.end=None
    db.add(PS)

    from odpw.db.models import Dataset as DDataset
    iter=DDataset.iter(dbm.getDatasetsAsStream(portalID=P.id, snapshot=snapshot))
    insertDatasets(P,db, iter,snapshot)
    s=db.Session
    PS= s.query(PortalSnapshot).filter(PortalSnapshot.portalid==P.id, PortalSnapshot.snapshot==snapshot).first()
    PS.datasetCount= s.query(Dataset).filter(Dataset.snapshot==snapshot).filter(Dataset.portalid==P.id).count()
    PS.resourceCount=s.query(Dataset).filter(Dataset.snapshot==snapshot).filter(Dataset.portalid==P.id).join(MetaResource,MetaResource.md5==Dataset.md5).count()

    s.commit()
    s.remove()

    try:
        aggregatePortalQuality(db,P.id, snapshot)
    except Exception as exc:
        ErrorHandler.handleError(log, "PortalFetchAggregate", exception=exc, pid=P.id, snapshot=snapshot, exc_info=True)


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


def insertDatasets(P, db, iter, snapshot, batch=100):

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
                DD=None
                with Timer(key='db.datasetdataExists(md5v)'):
                    process = not db.datasetdataExists(md5v)
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
                        MQs= createMetaResources(md5v,d)
                        for MR in MQs:
                            bulk_obj['mr'].append(MR)
                    except Exception as e:
                        pass
                        #print "AND AGAIN",md5v, db.datasetdataExists(md5v)
                #DATATSET
                D= Dataset(id=d.id,
                       snapshot=d.snapshot,
                       portalid=d.portal_id,
                       md5=md5v,
                       organisation=DD.organisation if DD else getOrganization(d))
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
    log.info("InsertedDatasets", parsed=c,snapshot=snapshot)


#--*--*--*--*
def help():
    return "perform head lookups"
def name():
    return 'FetchM'

def setupCLI(pa):
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=4)
    pa.add_argument('--pid', dest='portalid' , help="Specific portal id ")
    pa.add_argument('--sn', dest='snapshot' , help="Snapshot")

def cli(args,dbm):
    sn=args.snapshot

    dbConf= readDBConfFromFile(args.config)
    db= DBClient(dbm)

    tasks=[]
    if args.portalid:
        P = db.Session.query(Portal).filter(Portal.id==args.portalid).one()
        if P is None:
            log.warn("PORTAL NOT IN DB", portalid=args.portalid)
            return
        else:
            tasks.append((P, dbConf,sn))
    else:
        for P in db.Session.query(Portal):
            tasks.append((P, dbConf,sn))

    log.info("START FETCH", processors=args.processors, dbConf=dbConf, portals=len(tasks))

    pool = Pool(args.processors)
    for x in pool.imap(fetchMigrate,tasks):
        pid,sn =x[0].id, x[1]
        log.info("RECEIVED RESULT", portalid=pid, snapshot=sn)