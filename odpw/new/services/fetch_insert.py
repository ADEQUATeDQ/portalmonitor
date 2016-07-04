import datetime
import json

import rdflib
import urlnorm


from odpw.db.dbm import PostgressDBM
from odpw.analysers.quality.analysers import DCATDMD

from odpw.new.db import DBClient
from odpw.new.model import Portal, Dataset, DatasetData, Base, PortalSnapshot, MetaResource, \
    DatasetQuality
from odpw.new.portal_fetch_processors import CKAN
from odpw.new.test_md5 import md5
from odpw.utils.dataset_converter import CKANConverter, graph_from_opendatasoft, fix_socrata_graph
from odpw.utils.dcat_access import getModificationDate, getCreationDate, getDistributionLicenseTriples, \
    getDistributionDownloadURLs, getDistributionAccessURLs, getDistributionFormatWithURL, \
    getDistributionMediaTypeWithURL, getDistributionSizeWithURL, getDistributionCreationDateWithURL, \
    getDistributionModificationDateWithURL, getOrganization
from odpw.utils.licenses_mapping import LicensesOpennessMapping
from odpw.utils.timer import Timer

import structlog

from odpw.utils.util import ErrorHandler, getExceptionCode, getExceptionString

log =structlog.get_logger()


license_mapping = LicensesOpennessMapping()
def getLicense(dataset):
    values = getDistributionLicenseTriples(dataset)
    #for id, label, url in values:
    #    if id: return id;
    #    if url: return url
    #    if label: return label

    for id, label, url in values:
        id, appr = license_mapping.map_license(label, id, url)
        return id
    return None

def dict_to_dcat(dataset_dict, portal, graph=None, format='json-ld'):

    # init a new graph
    if not graph:
        graph = rdflib.Graph()

    if portal.software == 'CKAN':
        converter = CKANConverter(graph, portal.apiuri)
        converter.graph_from_ckan(dataset_dict)
    elif portal.software == 'Socrata':
        if 'dcat' in dataset_dict and dataset_dict['dcat']:
            graph.parse(data=dataset_dict['dcat'], format='xml')
            fix_socrata_graph(graph, dataset_dict, portal.apiuri)
            # TODO redesign distribution, format, contact (publisher, organization)
    elif portal.software == 'OpenDataSoft':
        graph_from_opendatasoft(graph, dataset_dict, portal.apiuri)
        # TODO contact, publisher, organization

    return json.loads(graph.serialize(format=format))


def normaliseFormat(v):
    if v is None:
        return None
    v = v.encode('utf-8').strip()
    v = v.lower()
    if v.startswith('.'):
        v = v[1:]
    return v


def toDatetime(value):
    if value:
        return datetime.datetime.strptime(value.split(".")[0], "%Y-%m-%dT%H:%M:%S")
    return None


def fetchHttp(P, snapshot, db):
    log.info("HTTPInsert", portalid=P.id, snapshot=snapshot)

    with Timer(key='InsertPortal', verbose=True):
        PS= PortalSnapshot(portalid=P.id, snapshot=snapshot)

        db.add(PS)
        try:
            iter=ckan.generateFetchDatasetIter(P, snapshot)
            insertDatasets(P,iter)
            PS.end=datetime.datetime.now()
            db.commit()
        except Exception as exc:
            ErrorHandler.handleError(log, "PortalFetchException", exception=exc, pid=P.id, snapshot=snapshot, exc_info=True)

            PS.end=datetime.datetime.now()
            PS.status=getExceptionCode(exc)
            PS.exc=getExceptionString(exc)
            db.commit()

def fetchMigrate(P,snapshot, db, dbm):

    PMD= dbm.getPortalMetaData(portalID=P.id, snapshot=snapshot)
    if PMD is None:
        print "Skipping ",P.id, 'in', snapshot
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
    insertDatasets(P,db, iter)

def createDatasetData(md5v,dataset):
    with Timer(key='createDatasetData'):
        DD= DatasetData(md5=md5v, raw=dataset.data)

        orga=getOrganization(dataset)

        cd=getCreationDate(dataset)
        md=getModificationDate(dataset)

        DD.modified     = toDatetime(cd[0] if len(cd)>0 else None)
        DD.created      = toDatetime(md[0] if len(md)>0 else None)
        DD.organisation = orga
        DD.license      = getLicense(dataset)
        return DD

def createDatasetQuality(P, md5v, dataset, ):
    with Timer(key='dict_to_dcat'):
        #analys quality
        dataset.dcat=dict_to_dcat(dataset.data, P)
    with Timer(key='quality'):
        q= DCATDMD()
        q.analyse_Dataset(dataset)
        q.done()

        q={ k.lower(): v['value'] for k,v in q.getResult().items()  }
        DQ=DatasetQuality(md5=md5v, **q)
        return DQ

def createMetaResources(md5v,dataset):
    with Timer(key='createMetaResources'):
        res= getDistributionAccessURLs(dataset)+getDistributionDownloadURLs(dataset)
        bulk_mr=[]
        uris=[]
        for uri in res:
            try:
                uri = urlnorm.norm(uri.strip())
            except Exception as e:
                log.warn("URIFormat", uri=uri, md5=md5v, msg=e.message)
                uri=uri

            f=getDistributionFormatWithURL(dataset, uri)
            m=getDistributionMediaTypeWithURL(dataset, uri)
            s=getDistributionSizeWithURL(dataset, uri)
            c=getDistributionCreationDateWithURL(dataset,uri)
            mod=getDistributionModificationDateWithURL(dataset,uri)
            if uri in uris:
                log.warn("WARNING, duplicate URI", dataset=dataset.id, md5=md5v, uri=uri,format=f,media=m)
                continue

            try:
                s=int(float(s)) if s is not None else None
            except Exception as e:
                s=None

            MR= MetaResource(
                uri = uri
                ,md5 = md5v
                ,media=m
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


def insertDatasets(P, db, iter, batch=1000):

    log.info("Inserting", portalid=P.id)

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

                with Timer(key='db.datasetdataExists(md5v)'):
                    process=not db.datasetdataExists(md5v)
                if process:
                    #DATATSET DATA
                    DD=createDatasetData(md5v,d)
                    db.add(DD) #primary key, needs to be inserted first


                    #DATATSET QUALTIY
                    DQ = createDatasetQuality(P, md5v, d)
                    bulk_obj['dq'].append(DQ)

                    #META RESOURCES
                    MQs= createMetaResources(md5v,d)
                    for MR in MQs:
                        bulk_obj['mr'].append(MR)

                #DATATSET
                D= Dataset(id=d.id,
                       snapshot=d.snapshot,
                       portalid=d.portal_id,
                       md5=md5v,
                       organisation=DD.organisation)
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
    log.info("InsertedDatasets", parsed=c)


