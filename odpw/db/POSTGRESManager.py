__author__ = 'jumbrich'

import psycopg2
import json
from datetime import date
from datetime import datetime

from odpw.db.models import Portal

import logging
logger = logging.getLogger(__name__)

import psycopg2.extras
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
psycopg2.extras.register_json(oid=3802, array_oid=3807, globally=True)

import urlnorm
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

    def __init__(self, db='portalwatch', host="localhost", port=5433):
        try:
            #Define our connection string
            conn_string = "dbname='%s' user='opwu' host='%s' port=%s " % (db, host,port)

            # print the connection string we will use to connect
            logger.info("Connecting to database\n	->%s" % (conn_string))
            self.con = psycopg2.connect(conn_string)

        except Exception as e:
            logger.critical("Unable to conntect to db(host=%s, db=%s)", host,db,exc_info=True)

    def initTables(self):
        logger.info("(Re)Initialising DB tables")
        #self.initUITable()
        self.initPortalsTable()
        #self.initPortalMetaDataTable()

     #PORTALS TABLE
    def initPortalsTable(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("DROP TABLE portals")
                cur.execute("CREATE TABLE IF NOT EXISTS  portals ("
                    "id VARCHAR (50) PRIMARY KEY,"
                    "url text,"
                    "apiurl text,"
                    "software text,"
                    "country text,"
                    "changefeed boolean,"
                    "status smallint,"
                    "datasets integer,"
                    "resources integer,"
                    "updated timestamp"
                    ");"
                )

    def insertPortal(self, Portal):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("INSERT INTO portals VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now())",
                    (Portal.id,
                    Portal.url,
                    Portal.apiurl,
                    Portal.software,
                    Portal.country,
                    Portal.changefeed,
                    Portal.status,
                    Portal.datasets,
                    Portal.resources
                    ))
        logger.info("Inserted %s into portals", Portal.id)

    def upsertPortal(self, Portal):
        """
        update or insert a portal
        update: if id exists, update datasets, resources and changefeed
        :param Portal:

        :return:
        """
        with self.con:
            with self.con.cursor() as cur:
                cur.execute(
                    "UPDATE portals SET changefeed=%s, status=%s,datasets=%s, resources=%s, updated=now() WHERE id=%s;"
                    "INSERT INTO portals (id,url,apiurl,software,country,changefeed, status,datasets,resources,updated)"
                    "SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,now() "
                    "WHERE NOT EXISTS (SELECT 1 FROM portals WHERE id=%s);",
                    (Portal.changefeed, Portal.status,Portal.datasets, Portal.resources, Portal.id, #update, id
                     Portal.id, Portal.url,Portal.apiurl,Portal.software,               #insert
                     Portal.country,Portal.changefeed,Portal.status,Portal.datasets,Portal.resources,  #insert
                     Portal.id
                    )
                )
        logger.info("Upserted %s into portals", Portal.id)

    def getPortals(self,maxDS=None, maxRes=None, software=None):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                where=''
                if maxDS:
                    where+=" AND datasets<="+str(maxDS)
                if maxRes:
                    where+=" AND resources<="+str(maxRes)
                if software:
                    where+=" AND software='"+software+"'"

                cur.execute("SELECT * FROM portals WHERE status !=404 "+where)
                return cur.fetchall()

    def getPortal(self, url=None, id=None, apiURL=None):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                where=''
                t=()
                if id:
                    where+=" id=%s"
                    t=t+(id,)
                elif url:
                    where+=" url=%s "
                    t = t+(url,)
                elif apiURL:
                    where+=" apiurl=%s"
                    t = t+(apiURL,)
                cur.execute("SELECT * FROM portals WHERE "+where,t)
                res = cur.fetchone()
                if res:
                    print res
                    return Portal.fromResult(res)
                return None


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







    ### Portal Meta Data
    def initPortalMetaDataTable(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("DROP TABLE portal_meta_data")
                cur.execute("CREATE TABLE IF NOT EXISTS  portal_meta_data ("
                    "id VARCHAR (50),"
                    "snapshot timestamp,"
                    "data JSONB,"
                    "PRIMARY KEY(id, snapshot)"
                    ");" )

    def upsertPortalMetaData(self, PortalMetaData):
        id=PortalMetaData.__dict__['portal_id']
        dict = PortalMetaData.__dict__
        if '_id' in dict:
            del dict['_id']

        with self.con:
            with self.con.cursor() as cur:
                data = json.dumps(nested_json(PortalMetaData.__dict__),default=date_handler)
                timestamp= psycopg2.TimestampFromTicks(PortalMetaData.__dict__['snapshot'])
                cur.execute(
                    "UPDATE portal_meta_data SET  data=%s WHERE id=%s AND snapshot=%s;"
                    "INSERT INTO portal_meta_data (id,snapshot, data) "
                    "SELECT %s, %s, %s"
                    "WHERE NOT EXISTS (SELECT 1 FROM portal_meta_data WHERE id=%s AND snapshot=%s);"
                    ,
                    ( data, id,timestamp,
                    id,timestamp, data,
                    id,timestamp
                    )
                )
    def getAllPortalMetaData(self, portalUrl):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                portal_id = get_portal_id_from_url(portalUrl)
                cur.execute("SELECT * FROM portal_meta_data WHERE id=%s",
                    (portal_id, )
                )
                logger.debug("SELECT * FROM portal_meta_data WHERE id=%s"% (portal_id))
                return cur.fetchall()

    def getPortalMetaData(self, portalUrl, portalSnapshot):
        portal_id = get_portal_id_from_url(portalUrl)
        timestamp= psycopg2.TimestampFromTicks(portalSnapshot)
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT data FROM portal_meta_data WHERE id=%s and snapshot=%s",
                    (portal_id,timestamp )
                )
                res = cur.fetchone()
                if res:
                    PMD = PortalMetaData(dict_string=res[0])
                else:
                    PMD = PortalMetaData(portalUrl, snapshot=portalSnapshot)
                    #self.upsertPortal(portal)
                return PMD

# INSERT INTO table (id, field, field2)
#        SELECT 3, 'C', 'Z'
#        WHERE NOT EXISTS (SELECT 1 FROM table WHERE id=3);

if __name__ == '__main__':
    p= PostGRESManager()
    #p.initTables()

