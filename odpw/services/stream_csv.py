import os
import pprint
from collections import defaultdict
from multiprocessing import Pool

import datetime

import requests
import structlog
from pyyacp import yacp

import rdflib
from rdflib import URIRef, BNode, Literal
from rdflib.namespace import Namespace, RDF, RDFS, DCTERMS, XSD

from odpw.core.dataset_converter import is_valid_uri
from odpw.quality.dqv_export import PW_AGENT
import hashlib

DCAT = Namespace("http://www.w3.org/ns/dcat#")
CSVW = Namespace("http://www.w3.org/ns/csvw#")
PROV = Namespace("http://www.w3.org/ns/prov#")



from odpw.core.db import DBManager
from odpw.csvanalyser import profiler

log = structlog.get_logger()


from odpw.core.api import DBClient
from odpw.utils.helper_functions import readDBConfFromFile
from odpw.utils.utils_snapshot import getCurrentSnapshot, tofirstdayinisoweek, toLastdayinisoweek
from odpw.core.model import Dataset, MetaResource, Portal

def addMetadata(url, snapshot, graph, max_lines=100, csvw_activity=None):
    timer = datetime.datetime.now()
    r = requests.get(url, stream=True, timeout=5)
    # BNode: url + snapshot
    bnode_hash = hashlib.sha1(url + str(snapshot))
    resource = BNode(bnode_hash.hexdigest())

    if is_valid_uri(url):
        ref = URIRef(url)
    else:
        ref = Literal(url)

    status_code = r.status_code
    if status_code == 200:
        # read first lines
        content = ''
        for i, line in enumerate(r.iter_lines()):
            content += line + '\n'
            if i >= max_lines:
                break
            if datetime.datetime.now() - timer > datetime.timedelta(seconds=3):
                break

        # initially test if we can parse the csv. if we fail, we do not create an entry
        parser = yacp.YACParser(content=content)

        # add url to graph
        graph.add((resource, CSVW.url, ref))

        if csvw_activity:
            graph.add((resource, PROV.wasGeneratedBy, csvw_activity))
            graph.add((csvw_activity, PROV.generated, resource))
            graph.add((resource, RDF.type, PROV.Entity))

        # add content type header information
        content_type = None
        if 'content-type' in r.headers:
            content_type = r.headers['content-type']
        elif 'Content-Type' in r.headers:
            content_type = r.headers['Content-Type']
        if content_type:
            try:
                # first part of content type
                t = content_type.split(';')[0]
                graph.add((resource, DCAT.mediaType, Literal(t)))
            except Exception as e:
                pass

        # add content length header information
        content_length = None
        if 'content-length' in r.headers:
            content_length = r.headers['content-length']
        elif 'Content-Length' in r.headers:
            content_length = r.headers['Content-Length']
        if content_length:
            try:
                # try to convert to numeric value
                l = float(content_length)
                graph.add((resource, DCAT.byteSize, Literal(l)))
            except Exception as e:
                pass

        data = profiler.profile(parser)

        # dialect
        # BNode: url + snapshot + CSVW.dialect
        bnode_hash = hashlib.sha1(url + str(snapshot) + CSVW.dialect.n3())
        dialect = BNode(bnode_hash.hexdigest())
        graph.add((resource, CSVW.dialect, dialect))
        graph.add((dialect, CSVW.encoding, Literal(data['encoding'])))
        graph.add((dialect, CSVW.delimiter, Literal(data['delimiter'])))
        if parser.description > 0:
            graph.add((dialect, CSVW.skipBlankRows, Literal(parser.description)))
        if data['header']:
            graph.add((dialect, CSVW.header, Literal(True)))
            graph.add((dialect, CSVW.headerRowCount, Literal(1)))
        else:
            graph.add((dialect, CSVW.header, Literal(False)))
            graph.add((dialect, CSVW.headerRowCount, Literal(0)))
        # columns
        # BNode: url + snapshot + CSVW.tableSchema
        bnode_hash = hashlib.sha1(url + str(snapshot) + CSVW.tableSchema.n3())
        tableschema = BNode(bnode_hash.hexdigest())
        graph.add((resource, CSVW.tableSchema, tableschema))
        col_i = 0
        for h, t in zip(data['header'], data['types']):
            # BNode: url + snapshot + CSVW.column + col_i
            bnode_hash = hashlib.sha1(url + str(snapshot) + CSVW.column.n3() + str(col_i))
            column = BNode(bnode_hash)
            graph.add((tableschema, CSVW.column, column))
            graph.add((column, CSVW.name, Literal(h)))
            graph.add((column, CSVW.datatype, t))
            col_i += 1
    # close request
    r.close()
    return status_code


def storeGraph(graph, portalid, directory):
    destination=os.path.join(directory, portalid + '.n3')
    graph.serialize(destination=destination, format='n3')
    log.info("CSVW Metadata graph stored", portal=portalid, destination=destination)


def csvw_prov(graph, snapshot):
    csvw_activity = URIRef("http://data.wu.ac.at/portalwatch/csvw/" + str(snapshot))
    graph.add((csvw_activity, RDF.type, PROV.Activity))
    graph.add((csvw_activity, PROV.startedAtTime, Literal(tofirstdayinisoweek(snapshot))))
    graph.add((csvw_activity, PROV.endedAtTime, Literal(toLastdayinisoweek(snapshot))))
    graph.add((csvw_activity, PROV.wasAssociatedWith, PW_AGENT))
    return csvw_activity


def streamCSVs(obj):
    P, dbConf, snapshot, dir = obj[0],obj[1],obj[2],obj[3]
    log.info("streamCSVs", portalid=P.id, snapshot=snapshot)

    dbm = DBManager(**dbConf)
    db = DBClient(dbm)

    s = db.Session
    q = s.query(MetaResource.uri).join(Dataset, MetaResource.md5 == Dataset.md5)\
        .filter(Dataset.snapshot == snapshot) \
        .filter(Dataset.portalid == P.id)\
        .filter(MetaResource.format == 'csv')

    graph = rdflib.Graph()

    csvw_activity = csvw_prov(graph, snapshot)

    exception = defaultdict(int)
    for res in q.all():
        url = res.uri
        try:
            url_status = addMetadata(url, snapshot, graph, csvw_activity=csvw_activity)
        except Exception as e:
            exception[type(e)] += 1

    log.info("CSVW Metadata Exceptions", portal=P.id, exceptions=str(dict(exception)))

    storeGraph(graph, P.id, dir)

    return P, snapshot



def help():
    return "read first x lines of CSV and collect CSVW metadata"
def name():
    return 'CSVW'

def setupCLI(pa):
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=1)
    pa.add_argument('--pid', dest='portalid' , help="Specific portal id ")
    pa.add_argument('--dir', dest='dir' , help="Directory where CSVW RDF will be stored", default='./csvw')

def cli(args,dbm):
    sn = getCurrentSnapshot()

    dbConf= readDBConfFromFile(args.config)
    db= DBClient(dbm)
    rdf_dir = args.dir

    if not os.path.exists(rdf_dir):
        os.mkdir(rdf_dir)
    sn_dir = os.path.join(rdf_dir, str(sn))
    if not os.path.exists(sn_dir):
        os.mkdir(sn_dir)

    tasks=[]
    if args.portalid:
        P =db.Session.query(Portal).filter(Portal.id==args.portalid).one()
        if P is None:
            log.warn("PORTAL NOT IN DB", portalid=args.portalid)
            return
        else:
            tasks.append((P, dbConf, sn, sn_dir))
    else:
        for P in db.Session.query(Portal):
            tasks.append((P, dbConf,sn, sn_dir))

    log.info("START FETCH", processors=args.processors, dbConf=dbConf, portals=len(tasks))

    pool = Pool(args.processors)
    for x in pool.imap(streamCSVs,tasks):
        pid,sn =x[0].id, x[1]
        log.info("RECEIVED RESULT", portalid=pid, snapshot=sn)