from sqlalchemy.dialects.postgresql.json import JSONB
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.expression import select
from sqlalchemy.sql.functions import func
from odpw.db.models import Dataset
from odpw.utils.util import progressIndicator, ErrorHandler
__author__ = 'jumbrich'

import structlog
log =structlog.get_logger()
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, VARCHAR, Boolean, SmallInteger,TIMESTAMP,BigInteger

class Bandersnatch(object):
    def __init__(self):
        conn_string = "postgresql://"
        conn_string += 'opwu'
        conn_string += ":0pwu"    
        conn_string += "@bandersnatch.ai.wu.ac.at"
        conn_string += ":5433"
        conn_string += "/portalwatch"
            
        print conn_string
        self.engine = create_engine(conn_string, pool_size=20)
        self.engine.connect()
            
        self.metadata = MetaData(bind=self.engine)
            
            
        ##TABLES
        self.portals = Table('portals',self.metadata,
                                Column('id', String(50), primary_key=True,unique=True),
                                Column('url', String),
                                 Column('apiurl', String),
                                 Column('software', String),
                                 Column('country', String),
                                 Column('status', SmallInteger),
                                 Column('changefeed', Boolean),
                                 Column('exception', String),
                                 Column('datasets', Integer),
                                 Column('resources', Integer),
                                 Column('latest_snapshot', String(7)),
                                 Column('updated', TIMESTAMP),
                                 )
            
       
        self.datasets = Table('datasets',self.metadata,
                            Column('dataset', String,primary_key=True),
                            Column('snapshot', String(7),primary_key=True,index=True),
                            Column('portal', String,primary_key=True,index=True),
                            
                            Column('data', JSONB),
                            Column('status', SmallInteger),
                            Column('exception', String),
                            Column('md5', String),
                            Column('change', SmallInteger),
                            Column('fetch_time', TIMESTAMP),
                            Column('qa_stats', JSONB),
                            Column('qa_time', TIMESTAMP)
                            )
        
    def getPortals(self):
        s = select([self.portals.c.id,self.portals.c.url,self.portals.c.apiurl])
        return s.execute()
    def countDatasets(self,portal=None):
        s=select([func.count(self.datasets.c.dataset)]).where(self.datasets.c.portal==portal)
        return s.execute().scalar()
    
    def getDatasets(self,portal=None):
        s=select([self.datasets.c.dataset,
                  self.datasets.c.snapshot,
                  self.datasets.c.data,
                  self.datasets.c.md5,
                  self.datasets.c.exception,
                  self.datasets.c.status
                  ]).where(self.datasets.c.portal==portal)
        return s.execute()


def convertSnapshot(snapshot):
    y=snapshot.split('-')[0]
    w=snapshot.split('-')[1]
    
    sn=str(y)[2:]+'{:02}'.format(int(w))
    return int(sn)

def name():
    return 'Migrate'
def help():
    return "Migration tool"

def setupCLI(pa):
    
    pa.add_argument("--out",  help='outputfolder to write the reports', dest='outdir')
    
    
    
def cli(args,dbm):
    
    urls={'http://data.bris.ac.uk/data': 'https://data.bris.ac.uk/data/',
    'http://data.hdx.rwlabs.org':'https://data.hdx.rwlabs.org/'}
    
    outdir= args.outdir
    if not outdir:
        print "No output dir "
        return
        
    #connect to ckan psql at bandersnatch
    b= Bandersnatch()
    
    
    #print convertSnapshot('2015-1')
    for p in b.getPortals():
        url=p['url']
        apiurl=p['apiurl']
        id=p['id']
        
        Portal = dbm.getPortal(url=url, apiurl=apiurl)
        if not Portal:
            Portal = dbm.getPortal(apiurl=apiurl)
            if not Portal:
                Portal = dbm.getPortal(url=url)
                if not Portal and url in urls:
                    Portal = dbm.getPortal(url=urls[url])
                    
        total = b.countDatasets(portal=id)
        steps= total/20 if total/20>0 else 1 
        
        if not Portal and total !=0:
            print "##No portal found for ", id,url, apiurl, total
        elif Portal:
            c=0
            print "Migrating ",total,"datasets for", Portal.id
            for ds in b.getDatasets(portal=id):
                c+=1
                try:
                    sn=convertSnapshot(ds['snapshot'])
                    data=ds['data']
                    status=ds['status']
                    exception=ds['exception']
                    md5=ds['md5']
                    if ds['data']:
                        did=ds['data']['id']
                    else:
                        did=ds['dataset']
                
                
                    d = Dataset(snapshot=sn, portalID=Portal.id, did=did, data=data,status=status, software=Portal.software, exception=exception, md5=md5)
                    dbm.insertDataset(d)
                except Exception as e:
                    ErrorHandler.handleError(log, "Migration", exception=e, pid=Portal.id, bdid=ds['dataset'],bpid=id)
                if c%steps==0:
                    progressIndicator(c,total, label=Portal.id)
                