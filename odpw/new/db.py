from multiprocessing.util import register_after_fork

import multiprocessing
import os
import sqlalchemy
from sqlalchemy import Column, String, Integer, ForeignKey, SmallInteger, TIMESTAMP, BigInteger, ForeignKeyConstraint, \
    Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import reflection
from sqlalchemy.orm import relationship, Session, scoped_session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.schema import DDL
from sqlalchemy.schema import (
    MetaData,
    Table,
    DropTable,
    ForeignKeyConstraint,
    DropConstraint,
    )

import structlog
from sqlalchemy.sql.ddl import DropConstraint

from odpw.new.model import DatasetData, DatasetQuality, Dataset, Base, Portal, PortalSnapshotQuality, PortalSnapshot, \
    tab_datasets, tab_resourcesinfo

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
            #register_after_fork(self.engine, self.engine.dispose)
            log.info("Connected DB")
            #self.engine.connect()

            self.session_factory = sessionmaker(bind=self.engine)#, expire_on_commit=False

            #self.session = self.Session()



            self.dataset_insert_function = DDL(
                """
                CREATE OR REPLACE FUNCTION dataset_insert_trigger()
                RETURNS TRIGGER AS $$

                DECLARE
                    _snapshot smallint;
                    _table_name text;

                BEGIN
                    _snapshot := NEW.snapshot;
                    _table_name := '"""+tab_datasets+"""_' || _snapshot;

                    PERFORM 1 FROM pg_tables WHERE tablename = _table_name;

                      IF NOT FOUND THEN
                        EXECUTE
                          'CREATE TABLE '
                          || quote_ident(_table_name)
                          || ' (CHECK ("snapshot" = '
                          || _snapshot::smallint
                          || ')) INHERITS ("""+tab_datasets+""")';

                        -- Indexes are defined per child, so we assign a default index that uses the partition columns
                        EXECUTE 'CREATE INDEX ' || quote_ident(_table_name||'_id') || ' ON '||quote_ident(_table_name) || ' (organisation)';
                      END IF;

                      EXECUTE
                        'INSERT INTO '
                        || quote_ident(_table_name)
                        || ' VALUES ($1.*)'
                      USING NEW;



                      RETURN NULL;
                END;

                $$ LANGUAGE plpgsql;
                """)
            self.dataset_insert_trigger = DDL(
                """
                CREATE TRIGGER dataset_insert_trigger
                BEFORE INSERT ON """+tab_datasets+"""
                FOR EACH ROW EXECUTE PROCEDURE dataset_insert_trigger();
                """
            )
            self.resourcesinfo_insert_function = DDL(
                """
                CREATE OR REPLACE FUNCTION resourcesinfo_insert_trigger()
                RETURNS TRIGGER AS $$

                DECLARE
                    _snapshot smallint;
                    _table_name text;

                BEGIN
                    _snapshot := NEW.snapshot;
                    _table_name := '"""+tab_resourcesinfo+"""_' || _snapshot;

                    PERFORM 1 FROM pg_tables WHERE tablename = _table_name;

                      IF NOT FOUND THEN
                        EXECUTE
                          'CREATE TABLE '
                          || quote_ident(_table_name)
                          || ' (CHECK ("snapshot" = '
                          || _snapshot::smallint
                          || ')) INHERITS ("""+tab_resourcesinfo+""")';

                        -- Indexes are defined per child, so we assign a default index that uses the partition columns
                        EXECUTE 'CREATE INDEX ' || quote_ident(_table_name||'_id') || ' ON '||quote_ident(_table_name) || ' (status)';
                      END IF;

                      EXECUTE
                        'INSERT INTO '
                        || quote_ident(_table_name)
                        || ' VALUES ($1.*)'
                      USING NEW;



                      RETURN NULL;
                END;

                $$ LANGUAGE plpgsql;
                """)
            self.resourcesinfo_insert_trigger = DDL(
                """
                CREATE TRIGGER resourcesinfo_insert_trigger
                BEFORE INSERT ON """+tab_resourcesinfo+"""
                FOR EACH ROW EXECUTE PROCEDURE resourcesinfo_insert_trigger();
                """
            )


    def db_DropEverything(self):
        print "Sroping everything"
        conn = self.engine.connect()

        # the transaction only applies if the DB supports
        # transactional DDL, i.e. Postgresql, MS SQL Server
        trans = conn.begin()

        inspector = reflection.Inspector.from_engine(self.engine)

        # gather all data first before dropping anything.
        # some DBs lock after things have been dropped in
        # a transaction.

        metadata = MetaData()

        tbs = []
        all_fks = []

        for table_name in inspector.get_table_names():
            fks = []
            for fk in inspector.get_foreign_keys(table_name):
                if not fk['name']:
                    continue
                fks.append(
                    ForeignKeyConstraint((),(),name=fk['name'])
                    )
            t = Table(table_name,metadata,*fks)
            tbs.append(t)
            all_fks.extend(fks)

        for fkc in all_fks:
            conn.execute(DropConstraint(fkc,cascade=True))

        for table in tbs:
            if table.name!=tab_datasets and table.name!=tab_resourcesinfo:
                conn.execute(DropTable(table))

        trans.commit()

    def init(self, Base):

        event.listen(Dataset.__table__, 'after_create', self.dataset_insert_function)
        event.listen(Dataset.__table__, 'after_create', self.dataset_insert_trigger)
        event.listen(Dataset.__table__, 'after_create', self.resourcesinfo_insert_function)
        event.listen(Dataset.__table__, 'after_create', self.resourcesinfo_insert_trigger)
        log.info("DROP ALL")
        Base.metadata.drop_all(self.engine)
        log.info("CREATE ALL")
        Base.metadata.create_all(self.engine)



class DBClient(object):


    def __init__(self, dbm):
        self.Session = scoped_session(dbm.session_factory)
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

    def add(self, obj):
        with self.session_scope() as session:
            session.add(obj)

    def merge(self, obj):
        with self.session_scope() as session:
            session.merge(obj)

    def bulkadd(self, obj):
        with self.session_scope() as session:
            session.bulk_save_objects(obj)

    def commit(self):
        self.Session.commit

    def portals(self):
        return Portal.query.all()

    def portalsSnapshots(self,snapshot):
        return PortalSnapshot.query\
            .filter(PortalSnapshot.snapshot==snapshot).all()

    def portalsQuality(self,snapshot):
        return PortalSnapshotQuality.query\
            .filter(PortalSnapshotQuality.snapshot==snapshot).all()

    def portalsAll(self,snapshot):
        return PortalSnapshot.query\
                .filter(PortalSnapshot.snapshot==snapshot)\
                .outerjoin( PortalSnapshotQuality, PortalSnapshot.portalid==PortalSnapshotQuality.portalid )\
                .join(Portal)\
                .add_entity(PortalSnapshotQuality)\
                .add_entity(Portal)\
                .all()

    def datasetdataExists(self, md5):
        return DatasetData.query.filter_by(md5=md5).first()

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