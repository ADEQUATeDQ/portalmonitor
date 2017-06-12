from sqlalchemy import Column, String, Integer, ForeignKey, SmallInteger, TIMESTAMP, BigInteger, ForeignKeyConstraint, \
    Boolean, func, select, Float, distinct
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import  backref
from sqlalchemy.orm import  relationship


import structlog
log =structlog.get_logger()


from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

tmp=''
tab_portals   =   tmp+'portals'
tab_portalevolution=tmp+'portalevolution'
tab_portalsnapshot=tmp+'portalsnapshot'
tab_portalsnapshotquality=tmp+'portalsnapshotquality'
tab_portalsnapshotdynamicity=tmp+'portalsnapshotdyn'
tab_portalsnapshotfetch=tmp+'portalsnapshotfetch'

tab_formatdist= tmp+"formatdist"
tab_licensedist= tmp+"licensedist"
tab_isodist= tmp+"licensedist"

tab_datasets=tmp+'datasets'
tab_datasetsquality=tmp+'datasetsquality'
tab_datasetsdata=tmp+'datasetsdata'

tab_resources=tmp+'metaresources'
tab_resourcesinfo=tmp+'resourcesinfo'
tab_resourcescrawllog=tmp+'resourcescrawllog'


tab_organisations=tmp+'organisations'
tab_organisationssnapshot=tmp+'organisationsnapshot'

tab_resourceshistory=tmp+'resourceshistory'
tab_resourcesfreshness=tmp+'resourcesfreshness'


class Portal(Base):
    __tablename__ = tab_portals

    id      = Column(String, primary_key=True, index=True,nullable=False)
    uri     = Column(String, nullable=False)
    apiuri  = Column(String)
    software = Column(String(12), nullable=False) # OpenDataSoft, CKAN, Socrata <13
    iso     = Column(String(2), nullable=False)
    active  = Column(Boolean, default=True,nullable=False)

    snapshots = relationship("PortalSnapshot", back_populates="portal")
    snapshotsquality = relationship("PortalSnapshotQuality", back_populates="portal")

    @hybrid_property
    def snapshot_count(self):
        print len(self.snapshots)
        return len(self.snapshots)
    @snapshot_count.expression

    def snapshot_count(cls):
        return select([func.count(PortalSnapshot.snapshot)])\
            .where(PortalSnapshot.portalid == cls.id).label("snapshot_count")

    @hybrid_property
    def first_snapshot(self):
        print [s for s in self.snapshots]
        return min([s.snapshot for s in self.snapshots])

    @first_snapshot.expression
    def first_snapshot(cls):
        return select([func.min(PortalSnapshot.snapshot)])\
            .where(PortalSnapshot.portalid == cls.id).label("first_snapshot")

    @hybrid_property
    def last_snapshot(self):
        return max([s.snapshot for s in self.snapshots])

    @last_snapshot.expression
    def last_snapshot(cls):
        return select([func.max(PortalSnapshot.snapshot)])\
            .where(PortalSnapshot.portalid == cls.id).label("last_snapshot")

    @hybrid_property
    def datasetcount(self):
        return self.snapshots.order_by(PortalSnapshot.snapshot.desc()).one().datasetcount

    @datasetcount.expression
    def datasetcount(cls):
        q=select([PortalSnapshot.datasetcount])\
            .where(PortalSnapshot.portalid == cls.id).order_by(PortalSnapshot.snapshot.desc()).limit(1).label("datasetcount")
        return q

    @hybrid_property
    def resourcecount(self):
        return self.snapshots.order_by(PortalSnapshot.snapshot.desc()).one().resourcecount

    @resourcecount.expression
    def resourcecount(cls):
        q=select([PortalSnapshot.resourcecount])\
            .where(PortalSnapshot.portalid == cls.id).order_by(PortalSnapshot.snapshot.desc()).limit(1).label("resourcecount")
        return q

    def __repr__(self):
        return "<Portal(id=%s, uri='%s', apiuri='%s', software='%s', iso=%s)>" % (
            self.id, self.uri, self.apiuri, self.software, self.iso)

class PortalSnapshot(Base):
    __tablename__ = tab_portalsnapshot

    portalid      = Column(String, ForeignKey(tab_portals+'.id'), primary_key=True, index=True,nullable=False)
    snapshot= Column( SmallInteger, primary_key=True)
    portal  = relationship("Portal", back_populates="snapshots")

    start       = Column(TIMESTAMP)
    end         = Column(TIMESTAMP)
    status      = Column(SmallInteger)
    exc         = Column(String)
    datasetcount    = Column(Integer)
    datasetsfetched    = Column(Integer)
    resourcecount   = Column(Integer)

    @hybrid_property
    def fetchtime(self):
        return self.end-self.start

    datasets = relationship("Dataset", back_populates="portalsnapshot")

    def __repr__(self):
        return "<PortalSnapshot(id=%s, snapshot=%s, start=%s, end=%s, status=%s,ds=%s,res=%s)>" % (
            self.portalid, self.snapshot, self.start, self.end, self.status,self.datasetcount,self.resourcecount)


class Serializable(object):
    __public__ = []

    def to_dict(self):
        d = {}
        for field in self.__public__:
            value = getattr(self, field)
            if value:
                d[field] = value
        return d

class PortalSnapshotDynamicity(Base,Serializable):
    __tablename__ = tab_portalsnapshotdynamicity

    portalid      = Column(String, ForeignKey(tab_portals+'.id'), primary_key=True, index=True,nullable=False)
    snapshot= Column( SmallInteger, primary_key=True)


    updated = Column(Integer)
    added = Column(Integer)
    deleted = Column(Integer)
    static =  Column(Integer)
    intersected =  Column(Integer)
    dindex = Column(Integer)
    changefrequ = Column(Float)
    size = Column(Integer)

    @hybrid_property
    def dyratio(self):
        return (self.added+self.deleted+self.updated)\
               /(1.0* self.intersected) if self.intersected >0 else 0

    @hybrid_property
    def adddelratio(self):
        return (self.added-self.deleted)\
               /(1.0* (self.added+self.deleted))if ((self.added+self.deleted)) >0 else 0

    @hybrid_property
    def addRatio(self):
        return (self.added) \
               / (1.0 * self.intersected)if self.intersected >0 else 0

    @hybrid_property
    def delRatio(self):
        return (self.deleted) \
               / (1.0 * self.intersected)if self.intersected >0 else 0

    @hybrid_property
    def updatedRatio(self):
        return ( self.updated) \
               / (1.0 * self.intersected)if self.intersected >0 else 0

    @hybrid_property
    def staticRatio(self):
        return (self.static) \
               / (1.0 * self.intersected)if self.intersected >0 else 0

class PortalSnapshotQuality(Base):
    __tablename__ = tab_portalsnapshotquality

    portalid      = Column(String, ForeignKey(tab_portals+'.id'), primary_key=True, index=True,nullable=False)
    snapshot= Column( SmallInteger, primary_key=True)
    portal  = relationship("Portal", back_populates="snapshotsquality")

    cocu = Column(Float)
    cocuN = Column(Integer)
    coce = Column(Float)
    coceN = Column(Integer)
    coda = Column(Float)
    codaN = Column(Integer)
    cofo = Column(Float)
    cofoN = Column(Integer)
    coli = Column(Float)
    coliN = Column(Integer)
    coac = Column(Float)
    coacN = Column(Integer)
    exda = Column(Float)
    exdaN = Column(Integer)
    exri = Column(Float)
    exriN = Column(Integer)
    expr = Column(Float)
    exprN = Column(Integer)
    exac = Column(Float)
    exacN = Column(Integer)
    exdi = Column(Float)
    exdiN = Column(Integer)
    exte = Column(Float)
    exteN = Column(Integer)
    exsp = Column(Float)
    exspN = Column(Integer)
    exco = Column(Float)
    excoN = Column(Integer)
    opfo = Column(Float)
    opfoN = Column(Integer)
    opma = Column(Float)
    opmaN = Column(Integer)
    opli = Column(Float)
    opliN = Column(Integer)
    datasets=Column(Integer)

    def __repr__(self):
        return "<PortalSnapshotQuality(id=%s, snapshot=%s, agg=%s)>" % (
            self.portalid, self.snapshot,  any([self.exda,self.coac,self.coce,self.cocu]))


class Dataset(Base):
    __tablename__ = tab_datasets

    id           = Column( String, primary_key=True)
    snapshot     = Column( SmallInteger, primary_key=True, index=True)
    portalid     = Column( String, primary_key=True, index=True)
    organisation = Column(String, index=True)
    title        = Column(String, index=True)
    md5          = Column(String, ForeignKey(tab_datasetsdata+'.md5'), index=True)


    __table_args__ = (ForeignKeyConstraint([portalid, snapshot],
                                           [tab_portalsnapshot+'.portalid',tab_portalsnapshot+'.snapshot']),
                      {})

    portalsnapshot  = relationship("PortalSnapshot", back_populates="datasets")
    data  = relationship("DatasetData", back_populates="dataset")

    def __repr__(self):
        return "<Dataset(id=%s, portalid='%s', snapshot=%s, md5=%s)>" % (
            self.id, self.portalid, self.snapshot, self.md5)

class DatasetData(Base):
    __tablename__ = tab_datasetsdata

    md5 = Column(String, primary_key=True, index=True, nullable=False)
    raw = Column(JSONB)
    dataset  = relationship("Dataset", back_populates="data")
    resources  = relationship("MetaResource", back_populates="dataset")

    modified = Column(TIMESTAMP)
    created = Column(TIMESTAMP)
    organisation = Column(String, index=True)
    license = Column(String, index=True)

    def __repr__(self):
        return "<DatasetData(md5=%s, data=%s)>" % (
            self.md5, self.raw is not None)

class DatasetQuality(Base):
    __tablename__ = tab_datasetsquality

    md5     = Column(String, ForeignKey(DatasetData.md5), primary_key=True, index=True)
    cocu = Column(Boolean)
    coce = Column(Boolean)
    coda = Column(Float)
    cofo = Column(Float)
    coli = Column(Boolean)
    coac = Column(Boolean)

    exda = Column(Float)
    exri = Column(Float)
    expr = Column(Float)
    exac = Column(Boolean)
    exdi = Column(Float)
    exte = Column(Float)
    exsp = Column(Float)
    exco = Column(Boolean)

    opfo = Column(Float)
    opma = Column(Float)
    opli = Column(Boolean)


    data = relationship("DatasetData", backref=backref("quality", uselist=False))

    def __repr__(self):
        return "<DatasetQuality(md5=%s, assessment=%s)>" % (
            self.md5, any([self.exda,self.coac,self.coce,self.cocu]))



class MetaResource(Base):
    __tablename__ = tab_resources

    uri = Column(String, primary_key=True, index=True)
    md5 = Column(String,ForeignKey(DatasetData.md5), primary_key=True,index=True )
    valid = Column(Boolean)
    format = Column(String)
    media = Column(String)
    size = Column(BigInteger)
    created = Column(TIMESTAMP)
    modified = Column(TIMESTAMP)


    dataset  = relationship("DatasetData", back_populates="resources")


    def info(cls):
        return select([ResourceInfo]).where(cls.uri == ResourceInfo.uri).where(cls.snapshot== ResourceInfo.snapshot)

    def __repr__(self):
        return "<Resource(uri=%s, dataset=%s)>" % (
            self.uri, self.md5)



class ResourceInfo(Base):
    __tablename__ = tab_resourcesinfo

    uri= Column(String, primary_key=True)
    snapshot= Column(SmallInteger, primary_key=True)
    timestamp= Column(TIMESTAMP)
    status=Column(SmallInteger)
    exc=Column(String)
    header=Column(JSONB)
    mime=Column(String)
    size=Column(BigInteger)


class ResourceCrawlLog(Base):
    __tablename__ = tab_resourcescrawllog

    uri= Column(String, primary_key=True)
    snapshot= Column(SmallInteger, primary_key=True)
    timestamp= Column(TIMESTAMP, primary_key=True)
    status=Column(SmallInteger, index=True)

    exc=Column(String)
    header=Column(JSONB)
    mime=Column(String)
    size=Column(BigInteger)
    crawltime=Column(BigInteger)

    referrer=Column( String)
    disklocation=Column( String)
    digest=Column( String)
    contentchanged=Column( Integer)
    domain=Column( String, index=True)



class ResourceHistory(Base):
    __tablename__ = tab_resourceshistory

    uri = Column(String, primary_key=True)
    snapshot = Column(SmallInteger, primary_key=True)
    md5 = Column(String,ForeignKey(DatasetData.md5), primary_key=True)
    modified = Column(TIMESTAMP)
    source = Column(String, primary_key=True)


class ResourceFreshness(Base):
    __tablename__ = tab_resourcesfreshness

    uri = Column(String, primary_key=True)
    snapshot = Column(SmallInteger, primary_key=True)
    md5 = Column(String,ForeignKey(DatasetData.md5), primary_key=True)

    a_cho_naive_header = Column(Float)
    a_cho_impr_header = Column(Float)
    a_emp_dist_header = Column(Float)
    mark1_header = Column(Float)
    mark2_header = Column(Float)

    a_cho_naive_metadata = Column(Float)
    a_cho_impr_metadata = Column(Float)
    a_emp_dist_metadata = Column(Float)
    mark1_metadata = Column(Float)
    mark2_metadata = Column(Float)

class FormatDist(Base):
    __tablename__ = tab_formatdist

    format = Column(String, primary_key=True)
    snapshot = Column(SmallInteger, primary_key=True)
    grouping = Column(String, primary_key=True)
    count = Column(Integer)
