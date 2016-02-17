'''
Created on Feb 16, 2016

@author: jumbrich
'''

import structlog
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, MetaData, ForeignKey, VARCHAR, Boolean, SmallInteger,TIMESTAMP,BigInteger, INTEGER

from sqlalchemy.dialects.postgresql import JSONB

from sqlalchemy.sql import select, text
from sqlalchemy import and_, func


log =structlog.get_logger()

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
            
        self.engine = create_engine(conn_string, pool_size=20, client_encoding='utf8')
        #add_engine_pidguard(self.engine)
        self.engine.connect()
            
        self.metadata = MetaData(bind=self.engine)
        
        ##TABLES
        self.schedule = Table('schedule',self.metadata,
                                 Column('uri', String),
                                 Column('experiment', String),
                                 Column('nextcrawltime', TIMESTAMP),
                                 Column('frequency', BigInteger)
                                 )
        
        self.crawllog = Table('crawllog',self.metadata,
                                 Column('uri', String),
                                 Column('experiment', String),
                                 Column('status', INTEGER),
                                 Column('timestamp', TIMESTAMP),
                                 Column('crawltime', BigInteger),
                                 Column('mime', String),
                                 Column('size', BigInteger),
                                 Column('referrer', String),
                                 Column('header', String),
                                 
                                 Column('disklocation', String),
                                 Column('digest', String),
                                 Column('contentchanged', INTEGER),
                                 Column('error', String),
                                 Column('crawlstart', TIMESTAMP),
                                 Column('domain', String),
                                 
                                 
                                 )
  
  
    
    def upsert(self,uri, experiment, nextcrawltime, frequency):
        with self.engine.begin() as con:
            
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
                con.execute(up)
            else:
                ins = self.schedule.insert().\
                           values(
                                uri=uri,
                                experiment= experiment,
                                nextcrawltime=nextcrawltime,
                                frequency=frequency
                                )
                con.execute(ins)
            
    def getOldSchedule(self, nextCrawltime):
        with self.engine.begin() as con:
            sel = select([self.schedule.c.uri,self.schedule.c.experiment, self.schedule.c.frequency]).where(self.schedule.c.nextcrawltime < nextCrawltime)
            return con.execute(sel)
    
    def getLatestURLInfo(self, url):
        with self.engine.begin() as con:
            latest= select([self.crawllog]).where(self.crawllog.c.uri == url).order_by(self.crawllog.c.crawltime).limit(1)
            return con.execute(latest)