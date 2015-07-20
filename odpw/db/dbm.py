from __future__ import generators

from _collections import defaultdict



__author__ = 'jumbrich'


import logging
logger = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()

import sys
import datetime
from odpw.timer import Timer
from odpw.db.models import Portal
from odpw.db.models import PortalMetaData
from odpw.db.models import Resource
from odpw.db.models import Dataset

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, VARCHAR, Boolean, SmallInteger,TIMESTAMP,BigInteger

from sqlalchemy.dialects.postgresql import JSONB

from sqlalchemy.sql import select, text
from sqlalchemy import and_, func
import math
import json

#import psycopg2.extras
#psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
#psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
#psycopg2.extras.register_json(oid=3802, array_oid=3807, globally=True)

from datetime import date
from collections import Mapping, Sequence
def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj

def nested_json(o):
    #print type(o)
    if isinstance(o, date):
            return str(o)
    if isinstance(o, float):
        if math.isnan(o):
            return None

        if math.isinf(o):
            return 'custom inf'
        return o
    elif isinstance(o, basestring):
        return o
    elif isinstance(o, Sequence):
        return [nested_json(item) for item in o]
    elif isinstance(o, Mapping):
        return dict((key, nested_json(value)) for key, value in o.iteritems())
    else:
        return o


class PostgressDBM:
    def __init__(self, db='portalwatch', host="localhost", port=5433, password='0pwu', user='opwu'):
        try:
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
            
            self.engine = create_engine(conn_string, pool_size=20)
            #self.conn = self.engine.connect()
            
            self.metadata = MetaData(bind=self.engine)
            #psycopg2.extras.register_json(oid=3802, array_oid=3807, globally=True)
            
            ##TABLES
            self.portals = Table('portals',self.metadata,
                                 Column('id', String(50), primary_key=True),
                                 Column('url', String),
                                 Column('apiurl', String),
                                 Column('software', String),
                                 Column('country', String),
                                 Column('changefeed', Boolean),
                                 Column('status', SmallInteger),
                                 Column('exception', String),
                                 Column('datasets', Integer),
                                 Column('resources', Integer),
                                 Column('latest_snapshot', String(7)),
                                 Column('updated', TIMESTAMP, onupdate=datetime.datetime.now, default=datetime.datetime.now)
                                 )
            
            self.pmd = Table('portal_meta_data',self.metadata,
                             Column('snapshot', String(7),primary_key=True),
                             Column('portal', String(50),primary_key=True),
                             Column('fetch_stats', JSONB),
                             Column('res_stats', JSONB),
                             Column('qa_stats', JSONB),
                             Column('general_stats', JSONB),
                             Column('resources', Integer),
                             Column('datasets', Integer),
                             )
            
            
            self.datasets = Table('datasets',self.metadata,
                            Column('dataset', String,primary_key=True),
                            Column('snapshot', String(7),primary_key=True),
                            Column('portal', String,primary_key=True),
                            Column('data', JSONB),
                            Column('status', SmallInteger),
                            Column('exception', String),
                            Column('md5', String),
                            Column('change', SmallInteger),
                            Column('fetch_time', TIMESTAMP),
                            Column('qa', JSONB),
                            Column('qa_time', TIMESTAMP)
                             )
            
            
            self.resources = Table('resources',self.metadata,
                            Column('url', String,primary_key=True),
                             Column('snapshot', String(7),primary_key=True),
                             Column('status', SmallInteger),
                             Column('exception', String),
                             Column('header', JSONB),
                             Column('mime', String),
                             Column('size', BigInteger),
                             Column('redirects', JSONB),
                             Column('origin', JSONB),
                             Column('timestamp', TIMESTAMP)
                             
                             
                             )
        except Exception as e:
            print "Unable to conntect to db(host=%s, db=%s)", host,db
            print e
            logger.critical("Unable to conntect to db(host=%s, db=%s)", host,db,exc_info=True)
    

    def initTables(self):  
        self.metadata.create_all(self.engine)    
            
    def insertPortal(self, Portal):
        with Timer(key="insertPortal") as t:
            ins = self.portals.insert().values(
                                               id=Portal.id,
                                               url=Portal.url,
                                               apiurl= Portal.apiurl,
                                               software=Portal.software,
                    country=Portal.country,
                    changefeed=Portal.changefeed,
                    status=Portal.status,
                    exception=Portal.exception,
                    datasets=Portal.datasets,
                    resources=Portal.resources,
                    latest_snapshot=Portal.latest_snapshot
                                               )
            self.log.debug(query=ins.compile(), params=ins.compile().params)
            ins.execute()
            #self.conn.execute(ins)
        self.log.info("INSERT INTO portals", pid=Portal.id)
    
    def updatePortal(self, Portal):
        with Timer(key="updatePortal") as t:
            ins = self.portals.update().\
                where(self.portals.c.id==Portal.id).\
                values(
                       url=Portal.url,
                       apiurl= Portal.apiurl,
                       software=Portal.software,
                       changefeed=Portal.changefeed,
                       status=Portal.status,
                       exception=Portal.exception,
                       datasets=Portal.datasets,
                       resources=Portal.resources,
                       latest_snapshot=Portal.latest_snapshot
                       )
            
            self.log.debug(query=ins.compile(), params=ins.compile().params)
            ins.execute()
            #self.conn.execute(ins)
        self.log.info("UPDATE INTO portals", pid=Portal.id)
    
    
    def getUnprocessedPortals(self,snapshot=None):
        with Timer(key="getUnprocessedPortals") as t:
            pmdid= select([self.pmd.c.portal]).where(and_(
                                                 self.pmd.c.snapshot==snapshot,
                                                 self.portals.c.id== self.pmd.c.portal
                                                 ))
            s = select([self.portals]).where(~self.portals.c.id.in_(pmdid)).order_by(self.portals.c.datasets)
            
            self.log.debug(query=s.compile(), params=s.compile().params)    
            
            return s.execute().fetchall()
            #return  self.conn.execute(s).fetchall()
        
            
    def getPortal(self, url=None, portalID=None, apiurl=None):
        with Timer(key="getPortal") as t:
            s = select([self.portals])
        
            if portalID:
                s= s.where(self.portals.c.id == portalID)
            if url:
                s= s.where(self.portals.c.url == url)
            if apiurl:
                s= s.where(self.portals.c.apiurl == apiurl)
        
            self.log.debug(query=s.compile(), params=s.compile().params)    
            
            res = s.execute().fetchone()
            
            if res:
                return Portal.fromResult(dict( res))
            return None
    
    def getPortals(self,maxDS=None, maxRes=None, software=None, status=None):
        with Timer(key="getPortals") as t:
            
            s = select([self.portals])
            if status:
                s=s.where(self.portals.c.status == status)
                    
            if maxDS:
                s=s.where(self.portals.c.datasets <= maxDS)
                
            if software:
                s=s.where(self.portals.c.software == software)
            
            self.log.debug(query=s.compile(), params=s.compile().params)
            
            return s.execute().fetchall() 
            #self.conn.execute(s).fetchall()

    def getPortalMetaData(self,portalID=None, snapshot=None):
        with Timer(key="getPortalMetaData") as t:
            s = select([self.pmd])
        
            if portalID:
                s= s.where(self.pmd.c.portal == portalID)
            if snapshot:
                s= s.where(self.pmd.c.snapshot == snapshot)
            
            self.log.debug(query=s.compile(), params=s.compile().params)    
            
            res = s.execute().fetchone()
            #self.conn.execute(s).fetchone()
        
            if res:
                return PortalMetaData.fromResult(dict( res))
            return None
        
    def getPortalMetaDatas(self, snapshot=None, portalID=None):
        with Timer(key="getPortalMetaDatas") as t:
            s = select([self.pmd])
            if snapshot:
                s=s.where(self.pmd.c.snapshot == snapshot)
            if portalID:
                s= s.where(self.pmd.c.portal == portalID)
                
            self.log.debug(query=s.compile(), params=s.compile().params)
               
            return s.execute().fetchall()

    def insertPortalMetaData(self, PortalMetaData):
        with Timer(key="insertPortalMetaData") as t:
            fetch_stats=None
            if PortalMetaData.fetch_stats:
                fetch_stats=PortalMetaData.fetch_stats
                #json.dumps(nested_json(PortalMetaData.fetch_stats),default=date_handler)
            general_stats=None
            if PortalMetaData.general_stats:
                general_stats=PortalMetaData.general_stats
                #json.dumps(nested_json(PortalMetaData.general_stats),default=date_handler)
            qa_stats=None
            if PortalMetaData.qa_stats:
                qa_stats=PortalMetaData.qa_stats
                #json.dumps(nested_json(PortalMetaData.qa_stats),default=date_handler)
            res_stats=None
            if PortalMetaData.res_stats:
                res_stats=PortalMetaData.res_stats
                #json.dumps(nested_json(PortalMetaData.res_stats),default=date_handler)
                
            ins = self.pmd.insert().values(
                                               snapshot=PortalMetaData.snapshot,
                                               portal=PortalMetaData.portal,
                                               fetch_stats=fetch_stats,
                                               res_stats=res_stats,
                                               qa_stats=qa_stats,
                                               general_stats=general_stats,
                                               resources=PortalMetaData.resources,
                                               datasets=PortalMetaData.datasets
                                               )
            self.log.debug(query=ins.compile(), params=ins.compile().params)
            ins.execute()
            #self.conn.execute(ins)
    
    def updatePortalMetaData(self, PortalMetaData):
        with Timer(key="updatePortalMetaData") as t:
            fetch_stats=None
            if PortalMetaData.fetch_stats:
                fetch_stats=PortalMetaData.fetch_stats
                #json.dumps(nested_json(PortalMetaData.fetch_stats),default=date_handler)
            general_stats=None
            if PortalMetaData.general_stats:
                general_stats=PortalMetaData.general_stats
                #json.dumps(nested_json(PortalMetaData.general_stats),default=date_handler)
            qa_stats=None
            if PortalMetaData.qa_stats:
                qa_stats=PortalMetaData.qa_stats
                #json.dumps(nested_json(PortalMetaData.qa_stats),default=date_handler)
            res_stats=None
            if PortalMetaData.res_stats:
                res_stats=PortalMetaData.res_stats
                #json.dumps(nested_json(PortalMetaData.res_stats),default=date_handler
                
            ins = self.pmd.update().where((self.pmd.c.snapshot==PortalMetaData.snapshot) & (self.pmd.c.portal== PortalMetaData.portal)).values(
                                               fetch_stats=fetch_stats,
                                               res_stats=res_stats,
                                               qa_stats=qa_stats,
                                               general_stats=general_stats,
                                               resources=PortalMetaData.resources,
                                               datasets=PortalMetaData.datasets
                                               )
            self.log.debug(query=ins.compile(), params=ins.compile().params)
            ins.execute()
            #self.conn.execute(ins) 
        
    def insertDatasetFetch(self, Dataset):
        with Timer(key="insertDatasetFetch") as t:
            change=2
            
            s=select([self.datasets.c.md5]).where(
                                                  and_(
                                                       self.datasets.c.dataset==Dataset.dataset, 
                                                       self.datasets.c.portal==Dataset.portal,
                                                       self.datasets.c.snapshot!=Dataset.snapshot)
                                                   ).order_by(self.datasets.c.snapshot.desc()).limit(1)
            self.log.debug(query=s.compile(), params=s.compile().params)
            result=s.execute()
            if result:
                for res in result:
                    if Dataset.md5 == res['md5'] :
                        change=0
            else: change=1
            
            #data=json.dumps(nested_json(Dataset.data),default=date_handler)
        
            ins = self.datasets.insert().values(
                                                dataset=Dataset.dataset, 
                                                portal=Dataset.portal,
                                                snapshot=Dataset.snapshot,
                                                data=Dataset.data,
                                                status=Dataset.status,
                                                exception=Dataset.exception,
                                                md5=Dataset.md5,
                                                change=change,
                                                fetch_time=datetime.datetime.now()
                                                
                                               )
            self.log.debug(query=ins.compile(), params=ins.compile().params)
            #self.conn.execute(ins)
            ins.execute()
        self.log.info("insertDatasetFetch INTO datasets", sn=Dataset.snapshot, did=Dataset.dataset, pid=Dataset.portal)

    def getDatasets(self,portalID=None, snapshot=None):
        with Timer(key="getDatasets") as t:
            s = select([self.datasets])
            
            if snapshot:
                s= s.where(self.datasets.c.snapshot == snapshot)
            if portalID:
                s= s.where(self.datasets.c.portal == portalID)
            
            self.log.debug(query=s.compile(), params=s.compile().params)    
            
            return s.execute()
            #return self.conn.execute(s)
    def countDatasets(self, portalID=None, snapshot=None):
        with Timer(key="countDatasets") as t:
            s = select([func.count(self.datasets.c.dataset)])
            if snapshot:
                s= s.where(self.datasets.c.snapshot == snapshot)
            if portalID:
                s= s.where(self.datasets.c.portal == portalID)
            
        self.log.debug(query=s.compile(), params=s.compile().params)
        return s.execute()
        #return  self.conn.execute(s)
            
    def getDataset(self, datasetID=None, snapshot=None, portal=None):
        with Timer(key="getDataset") as t:
            s = select([self.datasets])
            
            if datasetID:
                s= s.where(self.datasets.c.dataset == datasetID)
            if snapshot:
                s= s.where(self.datasets.c.snapshot == snapshot)
            if portal:
                s= s.where(self.datasets.c.portal == portal)
            
            self.log.debug(query=s.compile(), params=s.compile().params)    
            
            res = s.execute().fetchone()
            #self.conn.execute(s).fetchone()
        
            if res:
                return Dataset.fromResult( dict( res))
            return None

    def updateDataset(self, Dataset):
        with Timer(key="updateDataset") as t:
            
            data=None
            if Dataset.data:
                data = Dataset.data
            qa=None
            if Dataset.qa:
                qa = Dataset.qa
            up = self.datasets.update().where(
                                              and_(self.datasets.c.portal == Dataset.portal,
                                                   self.datasets.c.snapshot == Dataset.snapshot,
                                                   self.datasets.c.dataset == Dataset.dataset
                                                   )).\
                values(
                       dataset=Dataset.dataset, 
                       portal=Dataset.portal,
                       snapshot=Dataset.snapshot,
                       data=data,
                       status=Dataset.status,
                       exception=Dataset.exception,
                       md5=Dataset.md5,
                       change=Dataset.change,
                       fetch_time=Dataset.fetch_time,
                       qa=qa,
                       qa_time=Dataset.qa_time)
                                                
                                               
            self.log.debug(query=up.compile(), params=up.compile().params)
            up.execute()
            #self.conn.execute(up)
        self.log.info("insertDatasetFetch INTO datasets", sn=Dataset.snapshot, did=Dataset.dataset, pid=Dataset.portal)


    def insertResource(self, Resource):
        with Timer(key="insertResource") as t:
            origin=None
            if Resource.origin:
                origin=Resource.origin
                #json.dumps(nested_json(Resource.origin),default=date_handler)
            header=None
            if Resource.header:
                header=Resource.header
                #json.dumps(nested_json(Resource.header),default=date_handler)
            redirects=None
            if Resource.redirects:
                redirects=Resource.redirects
                #json.dumps(nested_json(Resource.redirects),default=date_handler)

            ins = self.resources.insert().values(
                                               status=Resource.status,
                                               url = Resource.url,
                                               snapshot = Resource.snapshot,
                                               origin=origin,
                                               header=header,
                                               mime=Resource.mime,
                                               size=Resource.size,
                                               timestamp=Resource.timestamp,
                                               redirects=redirects,
                                               exception=Resource.exception
                                               )
            self.log.debug(query=ins.compile(), params=ins.compile().params)
            #self.conn.execute(ins)
            ins.execute() 
        self.log.info("UPSERT INTO resources", sn=Resource.snapshot, url=Resource.url)

    def countProcessedResources(self, snapshot=None):
        with Timer(key="countProcessedResources") as t:
            s = select([func.count(self.resources.c.url)],\
                       and_(self.resources.c.snapshot==snapshot,
                            self.resources.c.status != None)
                       )
        self.log.debug(query=s.compile(), params=s.compile().params)
        return s.execute()
        #return  self.conn.execute(s)
      
    def getResources(self, snapshot=None):
        with Timer(key="getResources") as t:
            s = select([self.resources])
            if snapshot:
                s =s.where(self.resources.c.snapshot== snapshot)
            self.log.debug(query=s.compile(), params=s.compile().params)
            return s.execute()
            #return self.conn.execute(s)
        
    def getProcessedResources(self, snapshot=None):
        with Timer(key="getProcessedResources") as t:
            s = select([self.resources])
            if snapshot:
                s =s.where(self.resources.c.snapshot== snapshot)
            s=s.where(self.resources.c.status !=None)
            self.log.debug(query=s.compile(), params=s.compile().params)
            return s.execute()
            #return self.conn.execute(s)
        
    def getResourceWithoutHead(self, snapshot=None, status=None):
        with Timer(key="getResourceWithoutHead") as t:
            s=select([self.resources]).\
                where(self.resources.c.snapshot==snapshot).\
                where(self.resources.c.header == "null")
            if status:
                s= s.where(self.resources.c.status == status)
            if not status:
                s= s.where(self.resources.c.status == None)
            
            self.log.debug(query=s.compile(), params=s.compile().params)
            
            return s.execute()
            #return self.conn.execute(s)
            
    def getResource(self, Resource):
        with Timer(key="getResource") as t:
            s = select([self.resources])
        
            s= s.where(self.resources.c.url == Resource.url)
            
            s= s.where(self.resources.c.snapshot == Resource.snapshot)
            
            self.log.debug(query=s.compile(), params=s.compile().params)    
            
            
            res = s.execute().fetchone()
            #self.conn.execute(s).fetchone()
            if res:
                return Resource.fromResult( dict( res))
            return None
        
    def updateResource(self, Resource):
        with Timer(key="updateResource") as t:
            origin=None
            if Resource.origin:
                origin=Resource.origin
                #json.dumps(nested_json(Resource.origin),default=date_handler)
            header=None
            if Resource.header:
                header=Resource.header
                #json.dumps(nested_json(Resource.header),default=date_handler)
            redirects=None
            if Resource.redirects:
                redirects=Resource.redirects
                #json.dumps(nested_json(Resource.redirects),default=date_handler)

            ins = self.resources.update().where((self.resources.c.snapshot == Resource.snapshot) & (self.resources.c.url == Resource.url)).values(
                                               status=Resource.status,
                                               origin=origin,
                                               header=header,
                                               mime=Resource.mime,
                                               size=Resource.size,
                                               timestamp=Resource.timestamp,
                                               redirects=redirects,
                                               exception=Resource.exception
                                               )
            self.log.debug(query=ins.compile(), params=ins.compile().params)
            ins.execute()
            #self.conn.execute(ins) 
        self.log.info("UPSERT INTO resources", sn=Resource.snapshot, url=Resource.url)
          
          
    ##############
    # STATS
    ##############
    def datasetsPerSnapshot(self, portalID=None):
        with Timer(key="datasetsPerSnapshot") as t:
            s=select( [self.datasets.c.snapshot, func.count(self.datasets.c.dataset).label('datasets')]).\
            where(self.datasets.c.portal==portalID).group_by(self.datasets.c.snapshot)
            
            self.log.debug(query=s.compile(), params=s.compile().params)
            return s.execute()
            #return self.conn.execute(s)
    def resourcesPerSnapshot(self,portalID=None):
        with Timer(key="resourcesPerSnapshot") as t:
            s=select( [self.resources.c.snapshot, func.count(self.resources.c.url).label('resources')]).\
            where(self.resources.c.origin[portalID]!=None).group_by(self.resources.c.snapshot)
            
            self.log.debug(query=s.compile(), params=s.compile().params)
            return s.execute()
             
def name():
    return 'DB'

def setupCLI(pa):
    #pa.add_argument('--size', help='get table entries',action='store_true')
    pa.add_argument('-i','--init',  action='store_true', dest='dbinit')

    
def cli(args,dbm):

    #if args.size:
    #    dbm.printSize()

    if args.dbinit:
        while True:
            choice = raw_input("WARNING: Do you really want to init the DB? (This destroys all data): (Y/N)").lower()
            if choice == 'y':
                dbm.initTables()
                break
            elif choice == 'n':
                break
            else:
                sys.stdout.write("Please respond with 'y' or 'n' \n")                
                
                
if __name__ == '__main__':
    logging.basicConfig()
    p= PostgressDBM(host="bandersnatch.ai.wu.ac.at", port=5433)
    
    
    for r in p.getResourceWithoutHead(snapshot="2015-29", status=None):
        print r
        break
    #===========================================================================
    # c=0
    # for res in p.getResources(snapshot='2015-28'):
    #     r = Resource.fromResult( dict( res))
    #     p.updateResource(r)
    #     c+=1
    #     if c%1000 == 0:
    #         print c
    #===========================================================================
        

    #r = p.getResource(url='http://data.gov.au/storage/f/2013-12-02T03:04:43.895Z/agil20131129.kmz', snapshot='2015-24')
    #print r.url    
    
    #===========================================================================
    # dataset = p.getDataset(snapshot='2015-28', portal='data.wu.ac.at', datasetID='all_campus_rooms')
    # 
    # por=p.getUnprocessedPortals(snapshot="2015-30")
    # for po in por:
    #     portal = Portal.fromResult(dict(po))
    # print len(por)
    #===========================================================================
        
    
    r = Resource.newInstance(url="http://data.wu.ac.at/dataset/169e2d7c-41f6-493b-b229-88fac2a0321a/resource/5150029d-d4c9-472f-8d8a-4e28f456ae41/download/allcoursesandevents01s.csv", snapshot='2015-29')
    res = p.getResource(r)
    print r.url
    
    print res
    
    
    #print p.getPortal(url='http://www.test.com/')
    #po = Portal.newInstance(url='http://www.test.com/', apiurl='http://test')
     
    #p.insertPortal(po)
    
#===============================================================================
#     r = Resource(url="http://example.org", snapshot="2015-29")
#     r.updateOrigin(pid='test', did='test')
#     
#     #p.insertResource(r)
# 
#     r1 = p.getResource(url="http://example.org", snapshot="2015-29")
#      
#     r2 = p.getResource(url="http://data.wu.ac.at/dataset/fed3bae6-397c-4f4c-9c14-15aa8443d268/resource/d17a0d32-562a-4b37-9f32-ce06c4482583/download/allcampusrooms.csv" , snapshot="2015-28")
#     
#     print r2
#     r1.updateOrigin(pid='test', did='test2')
#     p.updateResource(r2)
#     
#     print "r2:",type(r2.origin)
#===============================================================================
    
    
    #po.datasets=10
    #p.updatePortal(po)
    #for res in p.getPortals(maxDS=50, status=200):
    #    print res
    #p.initPortalsTable()
    #p.initSnapshotStatusTable()
#     p.initDatasetsTable()
#     p.initPortalMetaDataTable()
#     p.initResourceTable()
    #P = p.getPortal(url='http://dados.gov.br')
    #p.upsertPortal(P)
    #p.initTables()
    #P= Portal.newInstance(apiurl='http://data.glasgow.gov.uk/api', url='http://data.edostate.gov.ng/')
    #p.insertPortal(P)
    #p.printSize()

    #p.initTables()





