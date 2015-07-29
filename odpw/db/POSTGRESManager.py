from __future__ import generators

__author__ = 'jumbrich'


import json
import sys
from datetime import date


from odpw.db.models import Portal, PortalMetaData
from odpw.db.models import Resource


import logging
logger = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()

import psycopg2.extras
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
psycopg2.extras.register_json(oid=3802, array_oid=3807, globally=True)

from odpw.utils.timer import Timer


##date handler and nested json functions are necessary to convert the mongodb content  into valid postgres json
def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj

class DateEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, date):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

import math
from collections import Mapping, Sequence

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


class PostGRESManager:
    def __init__(self, db='portalwatch', host="localhost", port=5433, password='0pwu', user='opwu'):
        try:
            self.tablesInit={
                'portals': self.initPortalsTable,
                'datasets': self.initDatasetsTable,
                'resources': self.initResourceTable,
                'portal_meta_data': self.initPortalMetaDataTable,
                'snapshot_stats': self.initSnapshotStatusTable,
            }

            #Define our connection string
            self.log = log.new()
            
            conn_string = "dbname='%s'" %db
            if user:
                conn_string += " user='%s'" %(user)
                
            if port:
                conn_string += " port=%s"%(port,)
                
            if host:
                conn_string += " host='%s'"%(host,)
                
            if password:
                conn_string += " password='%s'"%(password)
                
            
            

            print conn_string

            # print the connection string we will use to connect
            self.log.debug("Connecting to database", conn_string=conn_string)
            self.con = psycopg2.connect(conn_string)
            self.log.info("Connection established",host=host, port=port)


        except Exception as e:
            print "Unable to conntect to db(host=%s, db=%s)", host,db
            print e
            logger.critical("Unable to conntect to db(host=%s, db=%s)", host,db,exc_info=True)

    def initTables(self):

        self.log.info("(Re)Initialising DB tables")

        for table in self.tablesInit:
            self.tablesInit[table]()

        self.printSize()
        self.log.info("Initialised DB tables")

    
     #PORTALS TABLE

    def initPortalsTable(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS portals ")
                cur.execute("CREATE TABLE IF NOT EXISTS  portals ("
                    "id VARCHAR (50) PRIMARY KEY,"
                    "url text,"
                    "apiurl text,"
                    "software text,"
                    "country text,"
                    "changefeed boolean,"
                    "status smallint,"
                    "exception text,"
                    "datasets integer,"
                    "resources integer,"
                    "latest_snapshot VARCHAR(7),"
                    "updated timestamp"
                    ");"
                )
                self.log.debug(query=cur.query)
        self.log.info("INIT", table='portals')

    #timer
    def insertPortal(self, Portal):
        with self.con:
            with self.con.cursor() as cur,\
                    Timer(key="insertPortal") as t:
                cur.execute("INSERT INTO portals VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())",
                    (Portal.id,
                    Portal.url,
                    Portal.apiurl,
                    Portal.software,
                    Portal.country,
                    Portal.changefeed,
                    Portal.status,
                    Portal.exception,
                    Portal.datasets,
                    Portal.resources,
                    Portal.latest_snapshot
                    ))
                self.log.debug(query=cur.query)
        self.log.info("INSERT INTO portals", pid=Portal.id)

    #timer
    def upsertPortal(self, Portal):
        with self.con:
            with self.con.cursor() as cur,\
                    Timer(key="upsertPortal") as t:
                cur.execute(
                    "UPDATE portals SET changefeed=%s, status=%s,exception=%s, datasets=%s, resources=%s, updated=now(), latest_snapshot=%s WHERE id=%s;"
                    "INSERT INTO portals (id,url,apiurl,software,country,changefeed, status,exception,datasets,resources,latest_snapshot,updated)"
                    "SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now() "
                    "WHERE NOT EXISTS (SELECT 1 FROM portals WHERE id=%s);",
                    (Portal.changefeed,
                     Portal.status,
                     Portal.exception,
                     Portal.datasets,
                     Portal.resources,
                     Portal.latest_snapshot,
                     Portal.id, #update, id

                     Portal.id,
                     Portal.url,
                     Portal.apiurl,
                     Portal.software,               #insert
                     Portal.country,
                     Portal.changefeed,
                     Portal.status,
                     Portal.exception,
                     Portal.datasets,
                     Portal.resources,
                     Portal.latest_snapshot,  #insert
                     Portal.id
                    )
                )
            self.log.debug(query=cur.query)
        self.log.info("UPSERT INTO portals", pid=Portal.id)

    #timer
    def getPortals(self,maxDS=None, maxRes=None, software=None, status=None):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur,\
                    Timer(key="getPortals") as t:
                t=()
                s=[]
                if status:
                    s.append("status=%s")
                    t=t+(status,)
                if maxDS:
                    s.append("datasets<=%s")
                    t=t+(maxDS,)
                if maxRes:
                    s.append("resources<=%s")
                    t=t+(maxRes,)
                if software:
                    s.append("AND software=%s")
                    t=t+(software,)

                if len(s)==0:
                    cur.execute("SELECT * FROM portals")
                else:
                    cur.execute("SELECT * FROM portals WHERE "+' AND '.join(s),t)

                self.log.debug(query=cur.query)
                return cur.fetchall()

    #timer
    def getPortal(self, url=None, id=None, apiurl=None):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur,\
                    Timer(key="getPortal") as t:
                where=''
                t=()
                if id:
                    where+=" id=%s"
                    t=t+(id,)
                elif url:
                    where+=" url=%s "
                    t = t+(url,)
                elif apiurl:
                    where+=" apiurl=%s"
                    t = t+(apiurl,)
                cur.execute("SELECT * FROM portals WHERE "+where,t)
                self.log.debug(query=cur.query)
                res = cur.fetchone()
                if res:
                    return Portal.fromResult(res)
                return None

    ########################
    #SNAPSHOT STATUS TABLE
    ########################
    def initSnapshotStatusTable(self):
        with self.con:
            with self.con.cursor() as cur:
                
                
                
                
                cur.execute("DROP TABLE IF EXISTS snapshot_stats ")
                cur.execute("CREATE TABLE IF NOT EXISTS  snapshot_stats ("
                    "snapshot VARCHAR (7) PRIMARY KEY,"
                    "portal_stats JSONB,"
                    "dataset_stats JSONB,"
                    "resource_stats JSONB,"
                    "qa_stats JSONB"
                    ");"
                )
                self.log.debug(query=cur.query)
        self.log.info("INIT", table='snapshot_stats')

    def upsertSnapshotStats(self, status, sn):
        with self.con:
            with self.con.cursor() as cur:
                ps=json.dumps(nested_json(status['portal_stats']),default=date_handler)
                ds=json.dumps(nested_json(status['dataset_stats']),default=date_handler)
                rs=json.dumps(nested_json(status['resource_stats']),default=date_handler)
                qs=json.dumps(nested_json(status['qa_stats']),default=date_handler)
                
                cur.execute("UPDATE snapshot_stats SET " 
                            "portal_stats=%s,dataset_stats=%s, resource_stats=%s,qa_stats=%s "
                            "WHERE snapshot=%s;"
                            "INSERT INTO snapshot_stats (snapshot,portal_stats,dataset_stats,resource_stats,qa_stats) "
                            "SELECT %s,%s,%s,%s,%s"
                            "WHERE NOT EXISTS (SELECT 1 FROM snapshot_stats WHERE snapshot=%s);"
                            ,(ps,ds,rs,qs,sn,
                              sn,ps,ds,rs,qs,
                              sn)
                            )

    def updateTimeInSnapshotStatusTable(self, sn, key):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("UPDATE sn_status SET "+key+"=now() WHERE snapshot=%s;"
                    "INSERT INTO sn_status (snapshot,"+key+")"
                    "SELECT %s,now() "
                    "WHERE NOT EXISTS (SELECT 1 FROM sn_status WHERE snapshot=%s);",
                    (sn, sn,sn))
                logger.debug("[SQL] %s",cur.query)


    #DATASET TABLE
    def initDatasetsTable(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS datasets ")
                cur.execute("CREATE TABLE IF NOT EXISTS  datasets ("
                    "dataset TEXT,"
                    "snapshot VARCHAR (7), "
                    "portal TEXT,"
                    "data JSONB,"
                    "status smallint,"
                    "exception text,"
                    "md5 TEXT,"
                    "change smallint,"
                    "fetch_time timestamp,"
                    "qa JSONB,"
                    "qa_time timestamp,"
                    "PRIMARY KEY (dataset, snapshot,portal)"
                    ");"
                )
                self.log.debug(query=cur.query)
        self.log.info("INIT", table='datasets')

    #timer
    def insertDataset(self, Dataset):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur,\
                    Timer(key="insertDataset") as t:
                change=2
                cur.execute("SELECT md5, snapshot from datasets WHERE dataset=%s and portal=%s ORDER BY snapshot DESC LIMIT 1",(Dataset.dataset, Dataset.portal))
                res = cur.fetchone()
                self.log.debug(query=cur.query, results=len(res))

                if res:
                    if Dataset.md5 == res['md5']:
                        change=0
                    else: change=1

                cur.execute("INSERT INTO datasets (dataset,snapshot,portal, data, status, exception,md5, change, fetch_time) "
                            "SELECT %s,%s,%s,%s,%s,%s,%s,%s,now()",
                    (   Dataset.dataset,
                        Dataset.snapshot,
                        Dataset.portal,
                        json.dumps(nested_json(Dataset.data),default=date_handler),
                        Dataset.status,
                        Dataset.exception,
                        Dataset.md5,
                        change
                    ))
                self.log.debug(query=cur.query)
        self.log.info("INSERT INTO datasets", sn=Dataset.snapshot, did=Dataset.dataset, pid=Dataset.portal)

    #timer
    def upsertDatasetFetch(self, Dataset):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur,\
                    Timer(key="upsertDatasetFetch") as t:
                change=2
                cur.execute("SELECT md5, snapshot from datasets WHERE dataset=%s and portal=%s and snapshot!=%s ORDER BY snapshot DESC LIMIT 1",(Dataset.dataset, Dataset.portal,Dataset.snapshot))
                res = cur.fetchone()
                self.log.debug(query=cur.query)
                if res:
                    if Dataset.md5 == res['md5'] :
                        change=0
                    else: change=1

                data=json.dumps(nested_json(Dataset.data),default=date_handler)
                cur.execute("UPDATE datasets SET data=%s, status=%s, exception=%s,md5=%s, change=%s, fetch_time=now() WHERE dataset=%s AND snapshot=%s AND portal=%s;"
                            ""
                            "INSERT INTO datasets (dataset,snapshot,portal, data, status,exception, md5, change, fetch_time) "
                            "SELECT %s,%s,%s,%s,%s,%s,%s,%s,now()"
                            "WHERE NOT EXISTS (SELECT 1 FROM datasets WHERE dataset=%s AND snapshot=%s AND portal=%s);",
                    (   #update
                        data,Dataset.status,Dataset.exception,Dataset.md5,change,
                        Dataset.dataset,Dataset.snapshot,Dataset.portal,
                        #insert
                        Dataset.dataset,
                        Dataset.snapshot,
                        Dataset.portal,
                        data,
                        Dataset.status,
                        Dataset.exception,
                        Dataset.md5,
                        change,
                        #not exists
                        Dataset.dataset,Dataset.snapshot,Dataset.portal
                    ))
                self.log.debug(query=cur.query)
        self.log.info("UPSERT INTO datasets", sn=Dataset.snapshot, did=Dataset.dataset, pid=Dataset.portal)

    #UI VIEWS
    def initUITable(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("DROP TABLE ui_views")
                cur.execute("CREATE TABLE IF NOT EXISTS  ui_views ("
                    "view VARCHAR (50) PRIMARY KEY,"
                    "updated timestamp,"
                    "data JSON);" )

    def upsertUITable(self, view, data):
        print data
        with self.con:
            with self.con.cursor() as cur:
                cur.execute(
                    "UPDATE ui_views SET data=%s, updated=now() WHERE view=%s;"
                    "INSERT INTO ui_views (view,updated, data) "
                    "SELECT %s, now(), %s"
                    "WHERE NOT EXISTS (SELECT 1 FROM ui_views WHERE view=%s);",
                    ( json.dumps(data), view,
                    view,json.dumps(data),
                    view)
                )

    ########################
    ### Portal Meta Data
    ########################
    def initPortalMetaDataTable(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS portal_meta_data")
                cur.execute("CREATE TABLE IF NOT EXISTS  portal_meta_data ("
                    "snapshot VARCHAR (7), "
                    "portal TEXT,"
                    "fetch_stats JSONB,"
                    "res_stats  JSONB,"
                    "qa_stats  JSONB,"
                    "general_stats  JSONB,"
                    "PRIMARY KEY(snapshot, portal)"
                    ");" )
                self.log.debug(query=cur.query)
        self.log.info("INIT", table='portal_meta_data')

    #timer
    def insertPortalMetaData(self, PortalMetaData):
        with self.con:
            with self.con.cursor() as cur,\
                    Timer(key="insertPortalMetaData") as t:
                fetch_stats=json.dumps(nested_json(PortalMetaData.fetch_stats),default=date_handler)
                general_stats=json.dumps(nested_json(PortalMetaData.general_stats),default=date_handler)

                cur.execute("INSERT INTO portal_meta_data (snapshot, portal, fetch_stats, general_stats) "
                            "SELECT %s,%s,%s,%s",
                    (
                        PortalMetaData.snapshot,
                        PortalMetaData.portal,
                        fetch_stats,
                        general_stats
                    ))
                self.log.debug(query=cur.query)
        self.log.info("INSERT INTO portal_meta_data", sn=PortalMetaData.snapshot, pid=PortalMetaData.portal)

    #timer
    def upsertPortalMetaData(self,PortalMetaData):
        with self.con:
            with self.con.cursor() as cur,\
                    Timer(key="upsertPortalMetaData") as t:
                fetch_stats=None
                if PortalMetaData.fetch_stats:
                    json.dumps(nested_json(PortalMetaData.fetch_stats),default=date_handler)
                general_stats=None
                if PortalMetaData.general_stats:
                    general_stats=json.dumps(nested_json(PortalMetaData.general_stats),default=date_handler)
                qa_stats=None
                if PortalMetaData.qa_stats:
                    qa_stats=json.dumps(nested_json(PortalMetaData.qa_stats),default=date_handler)
                
                res_stats=None
                if PortalMetaData.res_stats:
                    res_stats=json.dumps(nested_json(PortalMetaData.res_stats),default=date_handler)

                cur.execute("UPDATE portal_meta_data SET fetch_stats=%s, general_stats=%s, qa_stats=%s, res_stats=%s WHERE snapshot=%s AND portal=%s;"
                            ""
                            "INSERT INTO portal_meta_data (snapshot, portal, fetch_stats, general_stats, qa_stats, res_stats) "
                            "SELECT %s,%s,%s,%s,%s,%s"
                            "WHERE NOT EXISTS (SELECT 1 FROM portal_meta_data WHERE snapshot=%s AND portal=%s);",
                    (   #update
                        fetch_stats,
                        general_stats,
                        qa_stats,
                        res_stats,
                        PortalMetaData.snapshot,
                        PortalMetaData.portal,
                        #insert
                        PortalMetaData.snapshot,
                        PortalMetaData.portal,
                        fetch_stats,
                        general_stats,
                        qa_stats,
                        res_stats,
                        #not exists
                        PortalMetaData.snapshot,
                        PortalMetaData.portal
                    ))
                self.log.debug(query=cur.query)
        self.log.info("UPSERT INTO portal_meta_data", sn=PortalMetaData.snapshot, pid=PortalMetaData.portal)

    #timer
    def getPortalMetaDatas(self,snapshot=None):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur,\
                    Timer(key="getPortalMetaDatas") as t:
                
                t=()
                s=[]
                if snapshot:
                    s.append("snapshot=%s")
                    t=t+(snapshot,)
                if len(s)==0:
                    cur.execute("SELECT * FROM portal_meta_data")
                else:
                    cur.execute("SELECT * FROM portal_meta_data WHERE "+' AND '.join(s),t)
                self.log.debug(query=cur.query)
                return cur.fetchall()
    
    #timer
    def getPortalMetaData(self,snapshot=None, portalID=None):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur,\
                    Timer(key="getPortalMetaData") as t:
                
                cur.execute("SELECT * FROM portal_meta_data WHERE snapshot=%s and portal=%s", (snapshot, portalID))
                self.log.debug(query=cur.query)
                
                res = cur.fetchone()
                if res:
                    return PortalMetaData.fromResult(res)
                return None
    

    ########################
    ### RESOURCE TABLE
    #######################
    def initResourceTable(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS resources")
                cur.execute("CREATE TABLE IF NOT EXISTS  resources ("
                    "url Text, "
                    "snapshot VARCHAR (7),"
                    "status smallint,"
                    "exception Text,"
                    "header JSONB,"
                    "mime TEXT,"
                    "size bigint,"
                    "redirects JSONB,"
                    "origin JSONB,"
                    "timestamp timestamp,"
                    "PRIMARY KEY(url, snapshot)"
                    ");" )
                self.log.debug(query=cur.query)
        self.log.info("INIT", table='portal_meta_data')

    def getResource(self, url=None, snapshot=None):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur,\
                    Timer(key="getResource") as t:
                cur.execute("SELECT * FROM resources WHERE url=%s and snapshot=%s",(url,snapshot))
                self.log.debug(query=cur.query)

                res = cur.fetchone()
                if res:
                    return Resource.fromResult(res)
                return None

    def updateResource(self,url=None, snapshot=None, props=None):
        with self.con:
            with self.con.cursor() as cur,\
                    Timer(key="updateResource") as t:
                print props
                try:
                    header=None
                    if props['header'] is not None:
                        header=json.dumps(nested_json(props['header']),default=date_handler)
                    redirects=None
                    if 'redirects' in props and props['redirects'] is not None:
                        redirects=json.dumps(nested_json(props['redirects']),default=date_handler)
                    cur.execute("UPDATE resources SET " 
                            "status=%s, header=%s, mime=%s, size=%s, timestamp=now(),redirects=%s, exception=%s WHERE snapshot=%s AND url=%s;",
                            ( props['status'],
                              header,
                              props['mime'],
                              props['size'],
                              redirects,
                              props['exception'], 
                              
                              snapshot, url
                              )
                            )
                    self.log.debug(query=cur.query)
                except Exception as e:
                    self.log.warning("Error", exc_info=True)
                
        self.log.info("Updated INTO resources", sn=snapshot, url=url)
        
    def upsertResource(self, Resource):
        with self.con:
            with self.con.cursor() as cur,\
                    Timer(key="upsertResource") as t:

                origin=None
                if Resource.origin:
                    origin=json.dumps(nested_json(Resource.origin),default=date_handler)
                header=None
                if Resource.header:
                    header=json.dumps(nested_json(Resource.header),default=date_handler)
                redirects=None
                if Resource.redirects:
                    redirects=json.dumps(nested_json(Resource.redirects),default=date_handler)

                cur.execute("UPDATE resources SET status=%s, origin=%s, header=%s, mime=%s,size=%s,timestamp=%s,redirects=%s, exception=%s WHERE snapshot=%s AND url=%s;"
                            ""
                            "INSERT INTO resources (url, snapshot, status, origin, header, mime, size, timestamp,redirects,exception) "
                            "SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s "
                            "WHERE NOT EXISTS (SELECT 1 FROM resources WHERE snapshot=%s AND url=%s);",
                    (   #update
                        Resource.status,
                        origin,
                        header,
                        Resource.mime,
                        Resource.size,
                        Resource.timestamp,
                        redirects,
                        Resource.exception,

                        #where update
                        Resource.snapshot,
                        Resource.url,
                        #insert
                        Resource.url,
                        Resource.snapshot,
                        Resource.status,
                        origin,
                        header,
                        Resource.mime,
                        Resource.size,
                        Resource.timestamp,
                        redirects,
                        Resource.exception,

                        #not exists
                        Resource.snapshot,
                        Resource.url
                    ))
                self.log.debug(query=cur.query)
        self.log.info("UPSERT INTO resources", sn=Resource.snapshot, url=Resource.url)

    ########################
    ### General functions
    #######################
    def printSize(self):
        with self.con:
            with self.con.cursor() as cur:
                print "Current table sizes:"
                for table in self.tablesInit:
                    cur.execute("SELECT count(*) from "+table)
                    print str(cur.fetchone()[0]).rjust(8),"rows in Table:"+table

    def selectQuery(self, query, tuple=None):
         with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if tuple:
                    cur.execute(query,tuple)
                else:
                    cur.execute(query)
                self.log.debug(query=cur.query)
                return cur.fetchall()
    
    def insertQuery(self, query, tuple=None):
         with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if tuple:
                    cur.execute(query,tuple)
                else:
                    cur.execute(query)
                self.log.debug(query=cur.query)
                

def name():
    return 'DB'

def setupCLI(pa):
    pa.add_argument('--size', help='get table entries',action='store_true')
    pa.add_argument('-i','--init',  action='store_true', dest='dbinit')
    
    
    

    
def cli(args,dbm):

    if args.size:
        dbm.printSize()

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
    p= PostGRESManager(host="bandersnatch.ai.wu.ac.at")
    #p.initPortalsTable()
    p.initSnapshotStatusTable()
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

