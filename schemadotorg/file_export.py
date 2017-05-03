import base64
import json
import os
import shutil
from multiprocessing import Pool

import datetime
import structlog
import xml.etree.ElementTree as ET

from sqlalchemy.orm import scoped_session, sessionmaker

from odpw.utils.error_handling import ErrorHandler
from odpw.utils.utils_snapshot import getCurrentSnapshot, tofirstdayinisoweek
import dcat_to_schemadotorg

log =structlog.get_logger()

from odpw.utils.helper_functions import readDBConfFromFile

from odpw.core.model import Dataset, Portal, DatasetData
from odpw.core.db import  DBManager
from odpw.core.api import DBClient



def create_datasetlist(datasetlist, dir):
    with open(dir + '/sitemap.xml.html', 'w') as f:
        f.write("<!DOCTYPE html>\n<html>\n<body>\n")
        f.write("<ul>\n")

        for (ds_name, ds) in datasetlist:
            f.write("<li><a href="+ ds +">"+ ds_name +"</a></li>")

        f.write("</ul>\n")
        f.write("</body>\n</html>")


def create_schemadotorg(doc, dataset_file):
    with open(dataset_file, 'w') as f:
        content = '<!DOCTYPE html><html><script type="application/ld+json">'
        content += json.dumps(doc)
        content += '</script></html>'
        f.write(content)


def generate_schemadotorg_files(obj):
    P, dbConf, snapshot, dir = obj[0],obj[1],obj[2],obj[3]
    sitemap_urls = []

    dbm = DBManager(**dbConf)
    session = scoped_session(sessionmaker(
        bind=dbm.engine
    ))

    log.info("Start schema.org files", portal=P.id)
    portal_dir = dir + '/' + P.id
    if not os.path.exists(portal_dir):
        os.mkdir(portal_dir)

    p = session.query(Portal).filter(Portal.id == P.id).first()
    q = session.query(Dataset) \
        .filter(Dataset.snapshot == snapshot) \
        .filter(Dataset.portalid == P.id)
    datasetlist = []
    i = 0

    for d in q.all():
        try:
            q = session.query(DatasetData) \
                .join(Dataset, DatasetData.md5 == Dataset.md5) \
                .filter(Dataset.snapshot == snapshot) \
                .filter(Dataset.portalid == P.id) \
                .filter(Dataset.id == d.id)
            data = q.first()
            doc = dcat_to_schemadotorg.convert(p, data.raw)

            dataset_filename = base64.urlsafe_b64encode(d.id)
            dataset_file = portal_dir + "/" + dataset_filename
            create_schemadotorg(doc, dataset_file)
            if not d.title:
                t = d.id
            else:
                t = d.title

            datasetlist.append((t, dataset_filename))
            dt = data.modified
            if dt != None and dt < datetime.datetime(year=1980, month=1, day=1):
                dt = None
            sitemap_urls.append((dataset_file, dt))
            i += 1
            if i % 50000 == 0:
                log.info("Processed datasets", datasets=str(i))

        except Exception as exc:
                ErrorHandler.handleError(log, "CreateSchema.orgFile", exception=exc, pid=P.id, snapshot=snapshot,
                                         exc_info=True)

    create_datasetlist(datasetlist, portal_dir)
    create_sitemap(sitemap_urls, portal_dir)

    dt = tofirstdayinisoweek(snapshot)
    return P, dt, snapshot


def create_sitemap(sitemap_urls, directory, location="http://data.wu.ac.at"):
    urlset = ET.Element('urlset')
    for ds_url, lastmodified in sitemap_urls:
        url = ET.SubElement(urlset, 'url')
        freq = ET.SubElement(url, 'changefreq')
        freq.text = 'weekly'
        if lastmodified and isinstance(lastmodified, datetime.datetime):
            lastmod = ET.SubElement(url, 'lastmod')
            lastmod.text = lastmodified.strftime('%Y-%m-%d')
        loc = ET.SubElement(url, 'loc')
        loc.text = location + '/' + ds_url
    tree = ET.ElementTree(urlset)
    tree.write(directory + '/sitemap.xml')


def create_portal_sitemapindex(portals, directory, location="http://data.wu.ac.at/odso"):
    urlset = ET.Element('sitemapindex')
    for pid, lastmodified in portals:
        url = ET.SubElement(urlset, 'sitemap')
        loc = ET.SubElement(url, 'loc')
        if lastmodified and isinstance(lastmodified, datetime.datetime):
            lastmod = ET.SubElement(url, 'lastmod')
            lastmod.text = lastmodified.strftime('%Y-%m-%d')
        loc.text = location+ '/' + pid + '/' + 'sitemap.xml'
        p_ide = ET.SubElement(url, 'pid')
        p_ide.text = pid
    tree = ET.ElementTree(urlset)
    tree.write(directory + '/sitemap.xml')


#--*--*--*--*
def help():
    return "Export datasets as schema.org and dump in files"
def name():
    return 'SchemaDotOrg'

def setupCLI(pa):
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=1)
    pa.add_argument('--pid', dest='portalid' , help="Specific portal id ")
    pa.add_argument('--directory', dest='directory' , help="Home directory for schema.org files", default='.')
    pa.add_argument('--sn', type=int, dest='sn', help="Snapshot")
    pa.add_argument('--sitemap', dest='sitemap', help="Build sitemap.xml only", action='store_true')


def cli(args,dbm):
    dbConf= readDBConfFromFile(args.config)
    db= DBClient(dbm)
    if not args.sn:
        sn = getCurrentSnapshot()
    else:
        sn = args.sn

    directory = args.directory

    tasks=[]
    if args.portalid:
        P =db.Session.query(Portal).filter(Portal.id==args.portalid).one()
        if P is None:
            log.warn("PORTAL NOT IN DB", portalid=args.portalid)
            return
        else:
            tasks.append((P, dbConf, sn, directory))
    else:
        for P in db.Session.query(Portal):
            tasks.append((P, dbConf, sn, directory))

    log.info("START FETCH", processors=args.processors, dbConf=dbConf, portals=len(tasks))

    portals = []
    pool = Pool(args.processors)
    for x in pool.imap(generate_schemadotorg_files, tasks):
        pid, lastmod, sn = x[0].id, x[1], x[2]
        portals.append((pid, lastmod))
        log.info("RECEIVED RESULT", portalid=pid)

    create_portal_sitemapindex(portals, directory)



