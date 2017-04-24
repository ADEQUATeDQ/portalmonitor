import pandas as pd
from sqlalchemy import and_
from sqlalchemy import func, exists
from sqlalchemy.orm import scoped_session

from odpw.core.db import row2dict
from odpw.core.model import PortalSnapshotQuality, ResourceCrawlLog, ResourceHistory
from odpw.utils.plots import qa

from odpw.core.model import DatasetData, DatasetQuality, Dataset, Base, Portal, PortalSnapshotQuality, PortalSnapshot, \
    tab_datasets, tab_resourcesinfo, ResourceInfo, MetaResource



class DBClient(object):


    def __init__(self, dbm=None, Session=None):
        if dbm is not None:
            self.Session = scoped_session(dbm.session_factory)
        elif Session:
            self.Session
        Base.query = self.Session.query_property()


    from contextlib import contextmanager

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        #session = self.Session()
        try:
            yield self.Session
            self.Session.flush()
            self.Session.commit()
        except:
            self.Session.rollback()
            raise
        #finally:
        #   self.Session.remove()

    def remove(self):
        self.Session.remove()

    #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
    ### ADD & COMMIT
    #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#


    def add(self, obj):
        with self.session_scope() as session:
            session.add(obj)

    def bulkadd(self, obj):
        with self.session_scope() as session:
            session.bulk_save_objects(obj)

    def commit(self):
        self.Session.commit

    #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
    ### EXISTS FUNCTIONS
    #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#

    def exist_datasetdata(self, md5):
        return DatasetData.query.filter_by(md5=md5).first()

    def exist_resourceinfo(self, uri, snapshot):
        return ResourceInfo.query.filter_by(uri=uri, snapshot=snapshot).first()

    def exist_metaresource(self, uri):
        return MetaResource.query.filter_by(uri=uri).first()

    def datasetqualityExists(self, md5):
        with self.session_scope() as session:
            return session.query(DatasetQuality).filter_by(md5=md5).first()

    def getContentLocation(self, uri=None, digest=None):
        with self.session_scope() as session:
            q= session.query(ResourceCrawlLog.disklocation).filter(ResourceCrawlLog.uri==uri).filter(ResourceCrawlLog.digest==digest).first()
            return q
    def getLastDigest(self, uri):
        with self.session_scope() as session:
            q= session.query(ResourceCrawlLog.digest).filter(ResourceCrawlLog.uri==uri).filter(ResourceCrawlLog.status==200).order_by(ResourceCrawlLog.timestamp.desc()).first()
            return q

    #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
    ### PORTALS
    #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
    def portals(self):
        with self.session_scope() as session:
            return session.query(Portal)

    def portalAll(self):
        with self.session_scope() as session:
            return session.query(Portal, Portal.snapshot_count,Portal.first_snapshot, Portal.last_snapshot, Portal.datasetCount, Portal.resourceCount)

    def portalsSnapshots(self, snapshot):
        with self.session_scope() as session:
            return session.query(PortalSnapshot)\
            .filter(PortalSnapshot.snapshot==snapshot)


    def portalsQuality(self,snapshot):
        with self.session_scope() as session:
            return session.query(PortalSnapshotQuality)\
            .filter(PortalSnapshotQuality.snapshot==snapshot)

    def portalsAll(self,snapshot):
        with self.session_scope() as session:
            return session.query( PortalSnapshot)\
                .filter(PortalSnapshot.snapshot==snapshot)\
                .outerjoin( PortalSnapshotQuality, PortalSnapshot.portalid==PortalSnapshotQuality.portalid )\
                .join(Portal)\
                .add_entity(PortalSnapshotQuality)\
                .add_entity(Portal)\


    #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
    ### PORTAL
    #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#

    def portal(self, portalid):
        with self.session_scope() as session:
            return session.query(Portal).filter(Portal.id==portalid)

    def portalSnapshot(self,snapshot, portalid):
        with self.session_scope() as session:
            return session.query(PortalSnapshot)\
                .filter(PortalSnapshot.snapshot==snapshot)\
                .filter(PortalSnapshot.portalid==portalid)

    def portalSnapshotQuality(self, portalid, snapshot):
        with self.session_scope() as session:
            return session.query(PortalSnapshotQuality)\
                .filter(PortalSnapshotQuality.portalid==portalid)\
                .filter(PortalSnapshotQuality.snapshot==snapshot)


    def statusCodeDist(self, snapshot,portalid=None):
        with self.session_scope() as session:
            q= session.query(ResourceInfo.status,func.count())\
                .join(MetaResource, ResourceInfo.uri==MetaResource.uri)\
                .join(Dataset,Dataset.md5==MetaResource.md5)\
                .filter(Dataset.snapshot==snapshot)\
                .filter(ResourceInfo.snapshot==snapshot)
            if portalid:
                q=q.filter(Dataset.portalid==portalid)

            q=q.group_by(ResourceInfo.status)
            q=q.order_by(func.count().desc())

            return q

    def organisationDist(self, snapshot, portalid=None):
        with self.session_scope() as session:
            q= session.query(Dataset.organisation,func.count().label('count'))\
                .filter(Dataset.snapshot==snapshot)
            if portalid:
                q=q.filter(Dataset.portalid==portalid)
            q=q.group_by(Dataset.organisation)
            q=q.order_by(func.count().desc())
            return q



    def formatDist(self, snapshot, portalid=None, iso=None, software=None):
        with self.session_scope() as session:
            q= session.query(MetaResource.format, func.count().label('count'))\
                .join(Dataset,Dataset.md5==MetaResource.md5)\
                .filter(Dataset.snapshot==snapshot)
            if portalid:
                q=q.filter(Dataset.portalid==portalid)
            if iso:
                q= q.join(Portal, Portal.id == Dataset.portalid)\
                .filter(Portal.iso==iso)
            if software:
                q= q.join(Portal, Portal.id == Dataset.portalid)\
                .filter(Portal.software==software)


            q=q.group_by(MetaResource.format)
            q=q.order_by(func.count().desc())
            return q


    def distinctFormats(self, snapshot, portalid=None):
        with self.session_scope() as session:
            q= session.query(MetaResource).distinct(MetaResource.format)\
                .join(Dataset,Dataset.md5==MetaResource.md5)\
                .filter(Dataset.snapshot==snapshot)
            if portalid:
                q=q.filter(Dataset.portalid==portalid)
            return q

    def licenseDist(self, snapshot, portalid=None):
        with self.session_scope() as session:
            q= session.query(DatasetData.license,func.count().label('count')).join(Dataset)\
                .filter(Dataset.snapshot==snapshot)
            if portalid:
                q=q.filter(Dataset.portalid==portalid)
            q=q.group_by(DatasetData.license)
            q=q.order_by(func.count().desc())
            return q

    def distinctLicenses(self, snapshot, portalid=None):
        with self.session_scope() as session:
            q= session.query(DatasetData).distinct(DatasetData.license)\
                .join(Dataset)\
                .filter(Dataset.snapshot==snapshot)
            if portalid:
                q=q.filter(Dataset.portalid==portalid)
            return q

    def validURLDist(self, snapshot,portalid=None):
        with self.session_scope() as session:
            q= session.query(MetaResource.valid, func.count().label('count'))\
                .join(Dataset,Dataset.md5==MetaResource.md5)\
                .filter(Dataset.snapshot==snapshot)
            if portalid:
                q=q.filter(Dataset.portalid==portalid)

            q=q.group_by(MetaResource.valid)
            q=q.order_by(func.count(MetaResource.valid).desc())

            return q

    #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
    ### DATASETS
    #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#

    def getDatasets(self, portalid=None, snapshot=None):
        with self.session_scope() as session:
            q= session.query(Dataset)
            if portalid:
                q=q.filter(Dataset.portalid==portalid)
            if snapshot:
                q=q.filter(Dataset.snapshot==snapshot)
            return q

    def getDatasetData(self,md5=None):
        with self.session_scope() as session:
            q= session.query(DatasetData).filter(DatasetData.md5==md5).first()
            return q



    def getUnfetchedResources(self,snapshot, portalid=None, batch=None):
        with self.session_scope() as session:
            q=session.query(MetaResource.uri)\
                .join(Dataset, Dataset.md5==MetaResource.md5)\
                .filter(Dataset.snapshot==snapshot)\
                .filter(MetaResource.valid==True)\
                .filter(
                #~exists().where(ResourceInfo.uri == MetaResource.uri).where(ResourceInfo.snapshot == snapshot)
                    ~exists().where(
                        and_( ResourceInfo.uri==MetaResource.uri, ResourceInfo.snapshot==snapshot))
                )
            if portalid:
                q=q.filter(Dataset.portalid==portalid)
            if batch:
                q=q.limit(batch)
            return q

    def getDataUnfetchedResources(self,snapshot, portalid=None, batch=None, format=None):
        with self.session_scope() as session:
            q=session.query(MetaResource.uri)\
                .join(Dataset, Dataset.md5==MetaResource.md5)\
                .filter(Dataset.snapshot==snapshot)\
                .filter(MetaResource.valid==True)\
                .filter(
                    ~exists().where(and_(MetaResource.uri== ResourceCrawlLog.uri, ResourceCrawlLog.snapshot==snapshot))
                )
            if portalid:
                q=q.filter(Dataset.portalid==portalid)
            if batch:
                q=q.limit(batch)
            if format:
                q=q.filter(MetaResource.format==format)
            return q

    def getResourceInfos(self, snapshot, portalid=None):
        with self.session_scope() as session:
            q= session.query(ResourceInfo)\
                .join(MetaResource, ResourceInfo.uri==MetaResource.uri)\
                .join(Dataset,Dataset.md5==MetaResource.md5)\
                .filter(Dataset.snapshot==snapshot)
            if portalid:
                q=q.filter(Dataset.portalid==portalid)
            return q


    def getMetaResource(self, snapshot, portalid=None):
        with self.session_scope() as session:
            q= session.query(MetaResource)\
                .join(Dataset, Dataset.md5==MetaResource.md5)\
                .filter(Dataset.snapshot==snapshot)
            if portalid:
                q=q.filter(Dataset.portalid==portalid)
            return q

    def getResourceInfoByURI(self, uri, snapshot):
        with self.session_scope() as session:
            q= session.query(ResourceInfo)\
                .filter(ResourceInfo.uri==uri)\
                .filter(ResourceInfo.snapshot==snapshot)
            return q

    def portalSnapshotQualityDF(self, portalid, snapshot):

        q= self.portalSnapshotQuality(portalid,snapshot)
        data=None
        for r in q:
            data=row2dict(r)
            break
        d=[]

        datasets= int(data['datasets'])
        for inD in qa:
            for k , v in inD['metrics'].items():
                k=k.lower()
                # TODO what to do if metric has no value?
                if data[k] != 'None':
                    value=float(data[k])
                    perc=int(data[k+'N'])/(datasets*1.0) if datasets>0 else 0
                    c= { 'Metric':k, 'Dimension':inD['dimension'],
                         'dim_color':inD['color'], 'value':value, 'perc':perc}
                    c.update(v)
                    d.append(c)
        return pd.DataFrame(d)

    def getResourcesHistory(self, uri, md5, source=None):
        with self.session_scope() as session:
            q= session.query(ResourceHistory)\
                .filter(ResourceHistory.uri==uri)\
                .filter(ResourceHistory.md5==md5)
            if source:
                q=q.filter(ResourceHistory.source==source)
            q=q.order_by(ResourceHistory.snapshot.asc())
            return q




#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
### PORTAL
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#

def portal(session, portalid):
    return session.query(Portal).filter(Portal.id==portalid)

def portalSnapshot(session,snapshot, portalid):
    return session.query(PortalSnapshot).query\
            .filter(PortalSnapshot.snapshot==snapshot)\
            .filter(PortalSnapshot.portalid==portalid)

def portalSnapshotQuality(session, portalid, snapshot):
    return session.query(PortalSnapshotQuality)\
            .filter(PortalSnapshotQuality.portalid==portalid)\
            .filter(PortalSnapshotQuality.snapshot==snapshot)


def statusCodeDist(session, snapshot,portalid=None):

    q= session.query(ResourceInfo.status,func.count())\
        .join(MetaResource, ResourceInfo.uri==MetaResource.uri)\
        .join(Dataset,Dataset.md5==MetaResource.md5)\
        .filter(Dataset.snapshot==snapshot)\
        .filter(ResourceInfo.snapshot==snapshot)
    if portalid:
        q=q.filter(Dataset.portalid==portalid)

    q=q.group_by(ResourceInfo.status)
    q=q.order_by(func.count().desc())

    print str(q)

    return q

def organisationDist(session, snapshot, portalid=None):
    q= session.query(Dataset.organisation,func.count().label('count'))\
        .filter(Dataset.snapshot==snapshot)
    if portalid:
        q=q.filter(Dataset.portalid==portalid)
    q=q.group_by(Dataset.organisation)
    q=q.order_by(func.count().desc())
    return q



def formatDist(session, snapshot, portalid=None):
    q= session.query(MetaResource.format, func.count().label('count'))\
        .join(Dataset,Dataset.md5==MetaResource.md5)\
        .filter(Dataset.snapshot==snapshot)
    if portalid:
        q=q.filter(Dataset.portalid==portalid)

    q=q.group_by(MetaResource.format)
    q=q.order_by(func.count().desc())
    return q

def distinctFormats(session, snapshot, portalid=None):

    q= session.query(MetaResource).distinct(MetaResource.format)\
        .join(Dataset,Dataset.md5==MetaResource.md5)\
        .filter(Dataset.snapshot==snapshot)
    if portalid:
        q=q.filter(Dataset.portalid==portalid)
    return q

def distinctOrganisations(session, snapshot, portalid=None):

    q= session.query(Dataset).distinct(Dataset.organisation)\
        .filter(Dataset.snapshot==snapshot)
    if portalid:
        q=q.filter(Dataset.portalid==portalid)
    return q

def licenseDist(session, snapshot, portalid=None):

    q= session.query(DatasetData.license,func.count().label('count')).join(Dataset)\
        .filter(Dataset.snapshot==snapshot)
    if portalid:
        q=q.filter(Dataset.portalid==portalid)
    q=q.group_by(DatasetData.license)
    q=q.order_by(func.count().desc())
    return q

def distinctLicenses(session, snapshot, portalid=None):

    q= session.query(DatasetData).distinct(DatasetData.license)\
        .join(Dataset)\
        .filter(Dataset.snapshot==snapshot)
    if portalid:
        q=q.filter(Dataset.portalid==portalid)
    return q

def validURLDist(session, snapshot,portalid=None):
    q= session.query(MetaResource.valid, func.count().label('count'))\
        .join(Dataset,Dataset.md5==MetaResource.md5)\
        .filter(Dataset.snapshot==snapshot)
    if portalid:
        q=q.filter(Dataset.portalid==portalid)

    q=q.group_by(MetaResource.valid)
    q=q.order_by(func.count(MetaResource.valid).desc())

    return q