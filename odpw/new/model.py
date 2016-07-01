from sqlalchemy import Column, String, Integer, ForeignKey, SmallInteger, TIMESTAMP, BigInteger, ForeignKeyConstraint, \
    Boolean, func, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, backref, column_property, synonym
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship


import structlog
log =structlog.get_logger()

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

tmp='tmp'
tab_portals   =   tmp+'portals'
tab_portalevolution=tmp+'portalevolution'
tab_portalsnapshot=tmp+'portalsnapshot'
tab_portalsnapshotfetch=tmp+'portalsnapshotfetch'

tab_datasets=tmp+'datasets'
tab_datasetsquality=tmp+'datasetsquality'
tab_datasetsdata=tmp+'datasetsdata'

tab_resources=tmp+'metaresources'
tab_resourcesinfo=tmp+'resourcesinfo'

tab_organisations=tmp+'organisations'
tab_organisationssnapshot=tmp+'organisationsnapshot'

class Portal(Base):
    __tablename__ = tab_portals

    id      = Column(String, primary_key=True, index=True,nullable=False)
    uri     = Column(String, nullable=False)
    apiuri  = Column(String)
    software = Column(String(12), nullable=False) # OpenDataSoft, CKAN, Socrata <13
    iso     = Column(String(2), nullable=False)
    active  = Column(Boolean, default=True,nullable=False)

    snapshots = relationship("PortalSnapshot", back_populates="portal")


    def __repr__(self):
        return "<Portal(id=%s, uri='%s', apiuri='%s', software='%s', iso=%s)>" % (
            self.id, self.uri, self.apiuri, self.software, self.iso)

class PortalSnapshot(Base):
    __tablename__ = tab_portalsnapshot

    portalid      = Column(String, ForeignKey(tab_portals+'.id'), primary_key=True, index=True,nullable=False)
    snapshot= Column( SmallInteger, primary_key=True)
    portal  = relationship("Portal", back_populates="snapshots")

    start   = Column(TIMESTAMP, server_default=func.now())
    end     = Column(TIMESTAMP)
    status  = Column(SmallInteger)
    exc     = Column(String)

    @hybrid_property
    def fetchtime(self):
        return self.end-self.start

    datasets = relationship("Dataset", back_populates="portalsnapshot")

    def __repr__(self):
        return "<PortalSnapshot(id=%s, snapshot=%s, start=%s, end=%s, status=%s)>" % (
            self.portalid, self.snapshot, self.start, self.end, self.status)


class Dataset(Base):
    __tablename__ = tab_datasets

    id      =   Column(String, primary_key=True)
    snapshot=   Column( SmallInteger, primary_key=True)
    portalid=   Column( String,     primary_key=True)
    organisation = Column(String, index=True)
    md5     =   Column(String, ForeignKey(tab_datasetsdata+'.md5'))


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

    md5 = Column(String, primary_key=True,index=True, nullable=False)
    raw = Column(JSONB)
    dataset  = relationship("Dataset", back_populates="data")
    resources  = relationship("MetaResource", back_populates="dataset")

    modified = Column(TIMESTAMP)
    created = Column(TIMESTAMP)
    organisation = Column(String, index=True)
    license = Column(String)

    def __repr__(self):
        return "<DatasetData(md5=%s, data=%s)>" % (
            self.md5, self.raw is not None)

class DatasetQuality(Base):
    __tablename__ = tab_datasetsquality

    md5     = Column(String, ForeignKey(DatasetData.md5), primary_key=True)
    cocu = Column(SmallInteger)
    coce = Column(SmallInteger)
    coda = Column(SmallInteger)
    cofo = Column(SmallInteger)
    coli = Column(SmallInteger)
    coac = Column(SmallInteger)

    exda = Column(SmallInteger)
    exri = Column(SmallInteger)
    expr = Column(SmallInteger)
    exac = Column(SmallInteger)
    exdi = Column(SmallInteger)
    exte = Column(SmallInteger)
    exsp = Column(SmallInteger)
    exco = Column(SmallInteger)

    opfo = Column(SmallInteger)
    opma = Column(SmallInteger)
    opli = Column(SmallInteger)

    data = relationship("DatasetData", backref=backref("quality", uselist=False))

    def __repr__(self):
        return "<DatasetQuality(md5=%s, assessment=%s)>" % (
            self.md5, any([self.exda,self.coac,self.coce,self.cocu]))



class MetaResource(Base):
    __tablename__ = tab_resources

    uri = Column(String, primary_key=True)
    md5 = Column(String,ForeignKey(DatasetData.md5), primary_key=True )
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
    exc=(String)
    header=Column(JSONB)
    mime=Column(String)
    size=Column(BigInteger)

