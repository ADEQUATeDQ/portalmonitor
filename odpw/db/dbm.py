from __future__ import generators
from sqlalchemy.sql.expression import join, exists


__author__ = 'jumbrich'


import structlog
log =structlog.get_logger()

import sys
import datetime
from odpw.utils.timer import Timer
from odpw.db.models import Portal,PortalMetaData,Resource,Dataset

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, VARCHAR, Boolean, SmallInteger,TIMESTAMP,BigInteger

from sqlalchemy.dialects.postgresql import JSONB

from sqlalchemy.sql import select, text
from sqlalchemy import and_, func
import math
import json


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


class DMManager(object):
    def __init__(self, db='datamonitor', host="137.208.51.23", port=5432, password=None, user='postgres'):
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
        self.engine.connect()
            
        self.metadata = MetaData(bind=self.engine)
        
        ##TABLES
        self.schedule = Table('schedule',self.metadata,
                                 Column('uri', String),
                                 Column('experiment', String),
                                 Column('nextcrawltime', TIMESTAMP),
                                 Column('frequency', BigInteger)
                                 )
    
    
    def upsert(self,uri, experiment, nextcrawltime, frequency):
        with Timer(key="upsert") as t:
            
            sel = select([self.schedule.c.uri,self.schedule.c.experiment]).where(and_(self.schedule.c.uri == uri, self.schedule.c.experiment == experiment ))
            #print sel.compile(), sel.compile().params

            r= sel.execute().first()
            if r:
                up = self.schedule.update().\
                    where(and_(self.schedule.c.uri == uri, self.schedule.c.experiment == experiment )).\
                    values(
                       uri=uri,
                       experiment= experiment,
                       nextcrawltime=nextcrawltime,
                       frequency=frequency
                       )
                up.execute()
            else:
                ins = self.schedule.insert().\
                           values(
                                uri=uri,
                                experiment= experiment,
                                nextcrawltime=nextcrawltime,
                                frequency=frequency
                                )
                ins.execute()
            
    def getOldSchedule(self, nextCrawltime):
        
        sel = select([self.schedule.c.uri,self.schedule.c.experiment, self.schedule.c.frequency]).where(self.schedule.c.nextcrawltime < nextCrawltime)
        return sel.execute()
    
class PostgressDBM(object):
    def __init__(self, db='portalwatch', host="localhost", port=5433, password='0pwu', user='opwu'):
        
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
            
            print conn_string
            self.engine = create_engine(conn_string, pool_size=20)
            self.engine.connect()
            
            self.metadata = MetaData(bind=self.engine)
            
            
            ##TABLES
            self.portals = Table('portals',self.metadata,
                                 Column('id', String(70), primary_key=True, index=True,unique=True),
                                 
                                 Column('url', String),
                                 Column('apiurl', String),
                                 Column('software', String),
                                 Column('iso3', String(3))
                                 )
            
            self.pmd = Table('pmd',self.metadata,
                             Column('snapshot', SmallInteger,primary_key=True,index=True),
                             Column('portal_id', String(70),primary_key=True,index=True),
                             
                             Column('fetch_stats', JSONB),
                             Column('res_stats', JSONB),
                             Column('qa_stats', JSONB),
                             Column('general_stats', JSONB),
                             Column('resources', Integer),
                             Column('datasets', Integer)
                             )
            
            self.datasets = Table('datasets',self.metadata,
                            Column('id', String,primary_key=True),
                            Column('snapshot', SmallInteger,primary_key=True,index=True),
                            Column('portal_id', String(70),primary_key=True,index=True),
                            
                            Column('data', JSONB),
                            Column('status', SmallInteger),
                            Column('exception', String),
                            Column('md5', String),
                            Column('change', SmallInteger),
                            Column('qa_stats', JSONB)
                            )
            
            self.resources = Table('resources',self.metadata,
                             Column('url', String,primary_key=True,index=True),
                             Column('snapshot', SmallInteger,primary_key=True,index=True),
                             
                             Column('timestamp', TIMESTAMP),
                             Column('status', SmallInteger,index=True),
                             Column('exception', String),
                             Column('header', JSONB),
                             Column('mime', String,index=True),
                             Column('size', BigInteger),
                             Column('origin', JSONB)
                             )

    def initTables(self):  
        self.metadata.drop_all(self.engine)
        self.metadata.create_all(self.engine)    
           
           
    def getSnapshots(self, portalID=None,apiurl=None):
        with Timer(key="getSnapshots") as t:
            
            s = select([self.pmd.c.portal_id,self.pmd.c.snapshot])
            
            if portalID:
                s= s.where(self.pmd.c.portal_id==portalID)
            if apiurl:
                s= s.where(self.pmd.c.apiurl==apiurl)
            
            s=s.distinct()
            
            self.log.debug(query=s.compile(), params=s.compile().params)
            return s.execute()
         
    def insertPortal(self, Portal):
        with Timer(key="insertPortal") as t:
            ins = self.portals.insert().values(
                                               id=Portal.id,
                                               url=Portal.url,
                                               apiurl= Portal.apiurl,
                                               software=Portal.software,
                                               iso3=Portal.iso3,
                                               )
            self.log.debug(query=ins.compile(), params=ins.compile().params)
            ins.execute()
    
    def updatePortal(self, Portal):
        with Timer(key="updatePortal") as t:
            ins = self.portals.update().\
                where(self.portals.c.id==Portal.id).\
                values(
                       url=Portal.url,
                       apiurl= Portal.apiurl,
                       software=Portal.software,
                       iso3=Portal.iso3
                       )
            
            self.log.debug(query=ins.compile(), params=ins.compile().params)
            ins.execute()
            #self.conn.execute(ins)
    
    
    def getUnprocessedPortals(self,snapshot=None):
        with Timer(key="getUnprocessedPortals") as t:
            pmdid= select([self.pmd.c.portal_id]).where(and_(
                                                 self.pmd.c.snapshot==snapshot,
                                                 self.portals.c.id== self.pmd.c.portal_id
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
                return Portal.fromResult(dict(res))
            return None
    
    def getPortals(self, software=None):
        with Timer(key="getPortals") as t:
            
            s = select([self.portals])
            if software:
                s=s.where(self.portals.c.software == software)
            
            self.log.debug(query=s.compile(), params=s.compile().params)
            
            return s.execute() 
            #self.conn.execute(s).fetchall()

    def getPortalsCount(self, software=None):
        s = select([func.count(self.portals.c.id)])
        if software:
            s=s.where(self.portals.c.software == software)
            
        self.log.debug(query=s.compile(), params=s.compile().params)
            
        return s.execute()

    def getPortalMetaData(self,portalID=None, snapshot=None):
        with Timer(key="getPortalMetaData") as t:
            s = select([self.pmd])
        
            if portalID:
                s= s.where(self.pmd.c.portal_id == portalID)
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
                s= s.where(self.pmd.c.portal_id == portalID)
                
            self.log.debug(query=s.compile(), params=s.compile().params)
               
            return s.execute().fetchall()

    def getPortalMetaDatasBySoftware(self, software, snapshot=None, portalID=None):
        with Timer(key="getPortalMetaDatasBySoftware") as t:

            j = join(self.pmd, self.portals, self.pmd.c.portal_id == self.portals.c.id)
            s = select([self.pmd]).select_from(j)
            s = s.where(self.portals.c.software == software)

            if snapshot:
                s = s.where(self.pmd.c.snapshot == snapshot)
            if portalID:
                s = s.where(self.pmd.c.portal_id == portalID)

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
                                               portal_id=PortalMetaData.portal_id,
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
                
            ins = self.pmd.update().where((self.pmd.c.snapshot==PortalMetaData.snapshot) & (self.pmd.c.portal_id== PortalMetaData.portal_id)).values(
                                               fetch_stats=fetch_stats,
                                               res_stats=res_stats,
                                               qa_stats=qa_stats,
                                               general_stats=general_stats,
                                               resources=PortalMetaData.resources,
                                               datasets=PortalMetaData.datasets
                                               )
            self.log.debug("updatePortalMetaData",query=ins.compile(), params=ins.compile().params)
            ins.execute()
            #self.conn.execute(ins) 
        
    def insertDatasetFetch(self, Dataset):
        with Timer(key="insertDatasetFetch") as t:
            #assuming we have a change
            change=2
            
            #TODO optimise query, get first the latest snapshot and check then for md5
            s=select([self.datasets.c.md5]).where(
                                                  and_(
                                                       self.datasets.c.id==Dataset.id, 
                                                       self.datasets.c.portal_id==Dataset.portal_id,
                                                       self.datasets.c.snapshot != Dataset.snapshot)
                                                   ).order_by(self.datasets.c.snapshot.desc()).limit(1)
            self.log.debug(query=s.compile(), params=s.compile().params)
            result=s.execute()
            if result:
                for res in result:
                    if Dataset.md5 == res['md5'] :
                        change=0
            else: 
                change=1
            
            
            ins = self.datasets.insert().values(
                                                id=Dataset.id, 
                                                portal_id=Dataset.portal_id,
                                                snapshot=Dataset.snapshot,
                                                data=Dataset.data,
                                                status=Dataset.status,
                                                exception=Dataset.exception,
                                                md5=Dataset.md5,
                                                change=change,
                                              )
            self.log.debug(query=ins.compile(), params=ins.compile().params)
            self.log.info("InsertDataset", pid=Dataset.portal_id, did=Dataset.id)
            #self.conn.execute(ins)
            ins.execute()

    def updateDatasetFetch(self, Dataset):
        with Timer(key="insertDatasetFetch") as t:
            change=2
            
            s=select([self.datasets.c.md5]).where(
                                                  and_(
                                                       self.datasets.c.dataset==Dataset.dataset, 
                                                       self.datasets.c.portal_id==Dataset.portal_id,
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
        
            ins = self.datasets.update().values(
                                                data=Dataset.data,
                                                status=Dataset.status,
                                                exception=Dataset.exception,
                                                md5=Dataset.md5,
                                                change=change,
                                                fetch_time=datetime.datetime.now()
                                               ).where(and_(self.datasets.c.dataset==Dataset.dataset, 
                                                       self.datasets.c.portal_id==Dataset.portal_id,
                                                       self.datasets.c.snapshot!=Dataset.snapshot
                                                ))
            self.log.debug(query=ins.compile(), params=ins.compile().params)
            #self.conn.execute(ins)
            ins.execute()
        
    
    def getDatasets(self,portalID=None, snapshot=None):
        with Timer(key="getDatasets") as t:
            s = select([self.datasets])
            
            if snapshot:
                s= s.where(self.datasets.c.snapshot == snapshot)
            if portalID:
                s= s.where(self.datasets.c.portal_id == portalID)
            
            self.log.debug(query=s.compile(), params=s.compile().params)    
            
            return s.execute()
            #return self.conn.execute(s)


    def getDatasetsBySoftware(self, software, portalID=None, snapshot=None):
        with Timer(key="getDatasetsBySoftware") as t:

            j = join(self.datasets, self.portals, self.datasets.c.portal_id == self.portals.c.id)
            s = select([self.datasets]).select_from(j)
            s = s.where(self.portals.c.software == software)

            if snapshot:
                s = s.where(self.datasets.c.snapshot == snapshot)
            if portalID:
                s = s.where(self.datasets.c.portal_id == portalID)

            self.log.debug(query=s.compile(), params=s.compile().params)
        return s.execute()

    def countDatasets(self, portalID=None, snapshot=None):
        with Timer(key="countDatasets") as t:
            s = select([func.count(self.datasets.c.dataset)])
            if snapshot:
                s= s.where(self.datasets.c.snapshot == snapshot)
            if portalID:
                s= s.where(self.datasets.c.portal_id == portalID)
            
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
                s= s.where(self.datasets.c.portal_id == portal)
            
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
                                              and_(self.datasets.c.portal_id == Dataset.portal,
                                                   self.datasets.c.snapshot == Dataset.snapshot,
                                                   self.datasets.c.dataset == Dataset.dataset
                                                   )).\
                values(
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
            
            ins = self.resources.insert().values(
                                               status=Resource.status,
                                               url = Resource.url,
                                               snapshot = Resource.snapshot,
                                               origin=origin,
                                               header=header,
                                               mime=Resource.mime,
                                               size=Resource.size,
                                               timestamp=Resource.timestamp,
                                               exception=Resource.exception
                                               )
            self.log.debug(query=ins.compile(), params=ins.compile().params)
            #self.conn.execute(ins)
            ins.execute() 
  
    def countProcessedResources(self, snapshot=None,portalID=None):
        with Timer(key="countProcessedResources") as t:
            s = select([func.count(self.resources.c.url)],\
                       and_(self.resources.c.snapshot==snapshot
                            #,self.resources.c.status != -1
                            )
                       )
            if portalID:
                s= s.where(self.resources.c.origin[portalID]!=None)
            
            print s.compile(), s.compile().params    
            self.log.debug(query=s.compile(), params=s.compile().params)
            return s.execute()
        #return  self.conn.execute(s)
      
    def getResources(self, snapshot=None, portalID=None, status =None):
        with Timer(key="getResources") as t:
            s = select([self.resources])
            if snapshot:
                s =s.where(self.resources.c.snapshot== snapshot)
            if portalID:
                s= s.where(self.resources.c.origin[portalID]!=None)
            if status:
                s= s.where(self.resources.c.status==status)
            self.log.debug(query=s.compile(), params=s.compile().params)
            return s.execute()
            #return self.conn.execute(s)
        
    def getProcessedResources(self, snapshot=None, portalID=None):
        with Timer(key="getProcessedResources") as t:
            s = select([self.resources])
            if snapshot:
                s =s.where(self.resources.c.snapshot== snapshot)
            if portalID:
                s= s.where(self.resources.c.origin[portalID]!=None)  
                 
            #s=s.where(self.resources.c.status !=-1)
            self.log.debug(query=s.compile(), params=s.compile().params)
            return s.execute()
            #return self.conn.execute(s)
        
    def getResourceWithoutHeadCount(self,snapshot=None, status=-1):
        with Timer(key="getResourceWithoutHead") as t:
            s=select([func.count(self.resources.c.url)]).\
                where(self.resources.c.snapshot==snapshot).\
                where(self.resources.c.status == status)
            return s.execute().scalar()
               
    def getResourceWithoutHead(self, snapshot=None, status=-1, limit=None):
        with Timer(key="getResourceWithoutHead") as t:
            s=select([self.resources]).\
                where(self.resources.c.snapshot==snapshot).\
                where(self.resources.c.status == status)
            if limit:
                s=s.limit(limit)
            #if status:
            #    s= s.where(self.resources.c.status == status)
            #if not status:
            #    s= s.where(self.resources.c.status == None)
            
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
            
            ins = self.resources.update().where((self.resources.c.snapshot == Resource.snapshot) & (self.resources.c.url == Resource.url)).values(
                                               status=Resource.status,
                                               origin=origin,
                                               header=header,
                                               mime=Resource.mime,
                                               size=Resource.size,
                                               timestamp=Resource.timestamp,
                                               exception=Resource.exception
                                               )
            self.log.debug(query=ins.compile(), params=ins.compile().params)
            ins.execute()
            #self.conn.execute(ins) 
          
          
    ##############
    # STATS
    ##############
    def datasetsPerSnapshot(self, portalID=None, snapshot=None):
        with Timer(key="datasetsPerSnapshot") as t:
            s=select( [self.datasets.c.snapshot, func.count(self.datasets.c.id).label('datasets')]).\
            where(self.datasets.c.portal_id==portalID)
            if snapshot:
                s=s.where(self.datasets.c.snapshot==snapshot)
            
            s=s.group_by(self.datasets.c.snapshot)
            
            self.log.debug(query=s.compile(), params=s.compile().params)
            return s.execute()
            #return self.conn.execute(s)
    def resourcesPerSnapshot(self,portalID=None, snapshot=None):
        with Timer(key="resourcesPerSnapshot") as t:
            s=select( [self.resources.c.snapshot, func.count(self.resources.c.url).label('resources')]).\
            where(self.resources.c.origin[portalID]!=None)
            if snapshot:
                s=s.where(self.resources.c.snapshot==snapshot)
            
            s=s.group_by(self.resources.c.snapshot)
            
            
            self.log.debug(query=s.compile(), params=s.compile().params)
            return s.execute()
             
             
             
    #####################
    # Statistics
    ########
    
    def getLatestPortalSnapshots(self):
        latest= select([self.pmd.c.portal_id.label('portal_id'),self.pmd.c.datasets,self.pmd.c.resources,
                        func.max(self.pmd.c.snapshot).label('max')]).group_by(self.pmd.c.portal_id)
        print latest
        return latest.execute()
    
    def getLatestPortalMetaDatas(self):
        with Timer(key="getLatestPortalMetaDatas") as t:
            
            latest= select([self.pmd.c.portal_id.label('portal_id'), func.max(self.pmd.c.snapshot).label('max')]).group_by(self.pmd.c.portal_id).alias()
            t1=self.pmd.alias()
            
            
            s=  select([t1]).select_from(join(t1,latest,and_(latest.c.portal_id==t1.c.portal_id,latest.c.max==t1.c.snapshot)))
            
            return s.execute()
    
    def getLatestPortalMetaData(self, portalID=None):
        with Timer(key="getLatestPortalMetaData") as t:
            
            latest= select([func.max(self.pmd.c.snapshot).label('max')]).where(self.pmd.c.portal_id==portalID).alias()
            t1=self.pmd.alias()
            
            s=  select([t1]).select_from(join(t1,latest,and_(portalID==t1.c.portal_id,latest.c.max==t1.c.snapshot)))
            
            res = s.execute().fetchone()
            #self.conn.execute(s).fetchone()
        
            if res:
                return PortalMetaData.fromResult(dict( res))
            return None
            
    
    def getSoftwareDist(self):
        with Timer(key="getSoftwareDist") as t:
            s=  select([self.portals.c.software, func.count(self.portals.c.id).label('count')]).group_by(self.portals.c.software)
            self.log.debug(query=s.compile(), params=s.compile().params)
            return s.execute()
    
    #def getPortalStatusDist(self):
    #    with Timer(key="getPortalStatusDist") as t:
    #        s=  select([self.portals.c.status, func.count(self.portals.c.id).label('count')]).group_by(self.portals.c.status)
    #        self.log.debug(query=s.compile(), params=s.compile().params)
    #        return s.execute()
        
    def getCountryDist(self):
        with Timer(key="getCountryDist") as t:
            s= select([ self.portals.c.iso3, func.count(self.portals.c.id).label('count'),func.substring( self.portals.c.url ,'^[^:]*://(?:[^/:]*:[^/@]*@)?(?:[^/:.]*\.)+([^:/]+)' ).label('tld')]).\
            group_by(self.portals.c.iso3, func.substring( self.portals.c.url ,'^[^:]*://(?:[^/:]*:[^/@]*@)?(?:[^/:.]*\.)+([^:/]+)' ))
            self.log.debug(query=s.compile(), params=s.compile().params)
            return s.execute()
        
    def getPMDStatusDist(self):
        with Timer(key="getPMDStatusDist") as t:
            s = select([ self.pmd.c.snapshot, self.pmd.c.fetch_stats['portal_status'].label('status'),func.count(self.pmd.c.portal_id).label('count')]).group_by(self.pmd.c.snapshot,self.pmd.c.fetch_stats['portal_status'])
            self.log.debug(query=s.compile(), params=s.compile().params)
            return s.execute()
            
    
def name():
    return 'DB'
def help():
    return "DB specific utils"
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
                
                
#if __name__ == '__main__':
#    logging.basicConfig()
 #   p= PostgressDBM(host="bandersnatch.ai.wu.ac.at", port=5433)
#    
 #   d = DBAnalyser(p.getPMDStatusDist)
 #   d.analyse()
    
  #  df= d.getDataFrame()
   # print df
    
    #print dftopk(df, column="count", k=3)
    
    #print dftopk(df, column="count", k=3, otherrow=True)
    
    
    #print dftopk(df, column="count", k=3, otherrow=True, percentage=True)
    #print DFtoListDict(dftopk(df, column="count", k=3, otherrow=True, percentage=True))
    
    #print top10pp.append(top10pp.sum(numeric_only=True), ignore_index=True)
    #print DFtoListDict(df)
    #print res.keys()
    
    
    
    #===========================================================================
    # for r in p.getResourceWithoutHead(snapshot="2015-30", status=None):
    #     print r
    # print "end loop"
    #     
    # exit
    #===========================================================================
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
        
    
    #===========================================================================
    # r = Resource.newInstance(url="http://data.wu.ac.at/dataset/169e2d7c-41f6-493b-b229-88fac2a0321a/resource/5150029d-d4c9-472f-8d8a-4e28f456ae41/download/allcoursesandevents01s.csv", snapshot='2015-29')
    # res = p.getResource(r)
    # print r.url
    # 
    # print res
    #===========================================================================
    
    
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





