import datetime
from multiprocessing import Pool

import structlog
log =structlog.get_logger()

import urlnorm

from odpw.quality.dcat_analysers import dcat_analyser
from odpw.utils.utils_snapshot import getCurrentSnapshot

from odpw.services.aggregates import aggregatePortalQuality

from odpw.utils.helper_functions import readDBConfFromFile, md5
from odpw.utils.error_handling import ErrorHandler, getExceptionCode, getExceptionString
from odpw.utils.timing import Timer

from odpw.core.model import Dataset, DatasetData, PortalSnapshot, MetaResource, \
    DatasetQuality, Portal
from odpw.core.dataset_converter import dict_to_dcat
from odpw.core.dcat_access import getDistributionAccessURLs, getDistributionDownloadURLs, getDistributionFormatWithURL, \
    getDistributionMediaTypeWithURL, getDistributionSizeWithURL, getDistributionCreationDateWithURL, \
    getDistributionModificationDateWithURL, getOrganization, getCreationDate, getModificationDate, getLicense, getTitle
from odpw.core.parsing import toDatetime, normaliseFormat
from odpw.core.portal_fetch_processors import getPortalProcessor
from odpw.core.db import  DBManager
from odpw.core.api import DBClient

def fetchHttp(obj):
    P, dbConf, snapshot = obj[0],obj[1],obj[2]
    log.info("HTTPInsert", portalid=P.id, snapshot=snapshot)

    dbm = DBManager(**dbConf)
    db = DBClient(dbm)

    with Timer(key='InsertPortal', verbose=True):
        PS= PortalSnapshot(portalid=P.id, snapshot=snapshot)

        db.add(PS)
        try:
            processor=getPortalProcessor(P)
            iter=processor.generateFetchDatasetIter(P,PS, snapshot)
            insertDatasets(P,db,iter,snapshot)
            status=200
            exc=None
        except Exception as exc:
            ErrorHandler.handleError(log, "PortalFetchException", exception=exc, pid=P.id, snapshot=snapshot, exc_info=True)
            status=getExceptionCode(exc)
            exc=getExceptionString(exc)

        #update the portalsnapshot object with dataset and resource count and end time
        s = db.Session
        PS = s.query(PortalSnapshot).filter(PortalSnapshot.portalid==P.id, PortalSnapshot.snapshot==snapshot).first()
        PS.datasetsFetched = s.query(Dataset).filter(Dataset.snapshot==snapshot).filter(Dataset.portalid==P.id).count()
        PS.datasetCount=PortalSnapshot.datasetCount
        PS.resourceCount =s.query(Dataset).filter(Dataset.snapshot==snapshot).filter(Dataset.portalid==P.id).join(MetaResource,MetaResource.md5==Dataset.md5).count()
        PS.end = datetime.datetime.now()
        PS.exc=exc
        PS.status=status

        s.commit()
        s.remove()

        try:
            aggregatePortalQuality(db,P.id, snapshot)
        except Exception as exc:
            ErrorHandler.handleError(log, "PortalFetchAggregate", exception=exc, pid=P.id, snapshot=snapshot, exc_info=True)

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
                        MQs= createMetaResources(md5v,d)
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

def cli(args,dbm):
    sn = getCurrentSnapshot()

    dbConf= readDBConfFromFile(args.config)
    db= DBClient(dbm)

    tasks=[]
    if args.portalid:
        P =db.Session.query(Portal).filter(Portal.id==args.portalid).one()
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
    for x in pool.imap(fetchHttp,tasks):
        pid,sn =x[0].id, x[1]
        log.info("RECEIVED RESULT", portalid=pid, snapshot=sn)