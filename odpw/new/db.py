from multiprocessing.util import register_after_fork

import os
import sqlalchemy
from sqlalchemy import Column, String, Integer, ForeignKey, SmallInteger, TIMESTAMP, BigInteger, ForeignKeyConstraint, \
    Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, Session, scoped_session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship


import structlog

from odpw.new.model import DatasetData, DatasetQuality, Dataset

log =structlog.get_logger()

from sqlalchemy import event
from sqlalchemy import exc

def add_engine_pidguard(engine):
    """Add multiprocessing guards.

    Forces a connection to be reconnected if it is detected
    as having been shared to a sub-process.

    """

    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info['pid'] = os.getpid()

    @event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            # substitute log.debug() or similar here as desired
            print(
                "Parent process %(orig)s forked (%(newproc)s) with an open "
                "database connection, "
                "which is being discarded and recreated." %
                {"newproc": pid, "orig": connection_record.info['pid']})
            connection_record.connection = connection_proxy.connection = None
            raise exc.DisconnectionError(
                "Connection record belongs to pid %s, "
                "attempting to check out in pid %s" %
                (connection_record.info['pid'], pid)
            )

class DBManager(object):

    def __init__(self, db='portalwatch', host="localhost", port=5432, password=None, user='opwu', debug=False):

            #Define our connection string
            self.log = log.new()

            conn_string = "postgresql://"
            if user:
                conn_string += user
            if password:
                conn_string += ":"+password
            if host:
                conn_string += "@"+host
            if port:
                conn_string += ":"+str(port)
            conn_string += "/"+db
            log.info("Connecting DB")

            self.engine = create_engine(conn_string, pool_size=20, client_encoding='utf8', echo=debug)
            add_engine_pidguard(self.engine)
            register_after_fork(self.engine, self.engine.dispose)
            log.info("Connected DB")
            #self.engine.connect()

            self.session_factory = sessionmaker(bind=self.engine)#, expire_on_commit=False

            #self.session = self.Session()


class DBClient(object):

    def __init__(self, dbm):
        self.dbm=dbm
        self.Session = scoped_session(self.dbm.session_factory)

    def init(self, Base):
        Base.metadata.drop_all(self.dbm.engine)
        Base.metadata.create_all(self.dbm.engine)


    from contextlib import contextmanager

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        #session = self.Session()
        try:
            yield self.Session
            self.Session.commit()
        except:
            self.Session.rollback()
            raise
        #finally:
        #   self.Session.remove()



    def add(self, obj):
        with self.session_scope() as session:
            session.add(obj)
        #self.session.commit()

    def bulkadd(self, obj):
        with self.session_scope() as session:
            session.bulk_save_objects(obj)

    def commit(self):
        self.Session.commit

    def datasetdataExists(self, md5):
        with self.session_scope() as session:
            return session.query(DatasetData).filter_by(md5=md5).first()

    def datasetqualityExists(self, md5):
        with self.session_scope() as session:
            return session.query(DatasetQuality).filter_by(md5=md5).first()


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