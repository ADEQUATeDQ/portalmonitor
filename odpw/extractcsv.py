__author__ = 'jumbrich'

from db.models import Portal, Resource

import util
from util import getSnapshot,getExceptionCode,ErrorHandler as eh
from db.POSTGRESManager import PostGRESManager
from timer import Timer
import math
import argparse
import os
import datetime
import sys,traceback

import logging
logger = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()
import json
import shutil


def getCrawlLog(dbm, url, start, end):
    for crawllog in dbm.selectQuery("SELECT * from crawllog where uri=%s and status>=200 and status <400 and timestamp>%s and timestamp<%s", (url,start, end)):
        if crawllog['status']>200:
            crawl=[]
            crawl.append(crawllog)
            c=0
            while True:
                c+=1
                for crawllog1 in dbm.selectQuery("SELECT * from crawllog where referrer=%s and status>=200 and status <400 and timestamp>%s and timestamp<%s LIMIT 1", (url,start, end)):
                    crawl.append(crawllog1)
                    if  crawllog1['status']==200:
                        return crawl
                if c>4:
                    return None
                        
        else:    
            #print "200", crawllog['contentchanged'], crawllog['timestamp']
            return crawllog
    return None  

def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj

def default(obj):
    """Default JSON serializer."""
    import calendar, datetime

    if isinstance(obj, datetime.datetime):
        if obj.utcoffset() is not None:
            obj = obj - obj.utcoffset()
    millis = int(
        calendar.timegm(obj.timetuple()) * 1000 +
        obj.microsecond / 1000
    )
    return millis

def name():
    return 'ExtractCSV'

def setupCLI(pa):
    getportals = pa.add_argument_group('Portal info', 'information about portals')
    getportals.add_argument('-o','--out_file', dest='outfile', help='store portal list')
    getportals.add_argument('--dmhost', dest='host')
    
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')

def cli(args,dbm):

    sn = getSnapshot(args)
    if not sn:
        return
    
    if not args.outfile:
        print "Specify directory"
        return;
    else:
        if not os.path.exists(args.outfile):
            os.mkdir(args.outfile)
    
    
    experiment='odwu'
    dm_dbm= PostGRESManager(db='datamonitor', host=args.host, port=5432, password='p0stgres', user='postgres')
    for portal in dbm.selectQuery("SELECT * FROM portal_meta_data WHERE portal='data.gv.at' ORDER BY snapshot DESC"):
        dir=os.path.join(args.outfile,portal['portal'])
        if not os.path.exists(dir):
            os.mkdir(dir)
        dir=os.path.join(dir,portal['snapshot'])
        if not os.path.exists(dir):
            os.mkdir(dir) 
        
        print portal['portal'],portal['snapshot']
        
        d = portal['snapshot']
        start = datetime.datetime.strptime(d + '-1', "%Y-%W-%w")# monday of this week 
        end = start + datetime.timedelta(days=7)
        
        meta=[]
        
        formats=[]
        c=0
        for dataset in dbm.selectQuery("SELECT * from datasets WHERE portal=%s and snapshot=%s",(portal['portal'],portal['snapshot'])):
            #print c
            
            c+=1
            if 'data' in dataset:
                try:
                    data = dataset['data']
                    if data and "resources" in data:
                        for res in data['resources']:
                            format = res['format'].lower()
                            if 'csv' in format or 'xlsx' in format:
                                crawllog = getCrawlLog(dm_dbm, res['url'], start, end)
                                if crawllog:
                                    meta.append(
                                    {   'uri':res['url'],
                                        'meta':dataset,
                                        'crawllog':crawllog
                                    }
                                    )
                            else:
                                if format not in formats:
                                    formats.append(format)
                except Exception as e:
                    print(traceback.format_exc())
       
        for format in formats: 
            print "check ", format
        
        mfiles=[]
        for m in meta:
            
            disk = None
            if isinstance(m['crawllog'], dict):
                if m['crawllog']['status'] == 200:
                        disk=m['crawllog']['disklocation']
            else:
                for log in m['crawllog']:
                    print log
                    if log['status'] == 200:
                        disk=log['disklocation']
                 
            path=['/nfsbackup/data/datamonitor/',
                  '/nfsbackup/data/datamonitor/data/',
                  '/nfsbackup/data/datamonitor/2015/']
            
            for p in path:
                data= disk.replace("/data/datamonitor/data/",p )
                if os.path.isfile(data):
                    todir=disk.split("/")[-2]
                    tofile=disk.split("/")[-1]
            
                    ddir= os.path.join(dir, todir)
                    if not os.path.exists(ddir):
                        os.mkdir(ddir)
                    ddir= os.path.join(ddir, tofile)
            
                    print "copy", disk,"to",ddir    
                    shutil.copyfile(data, ddir)
                    
                    if isinstance(m['crawllog'], dict):
                        if m['crawllog']['status'] == 200:
                            m['crawllog']['disklocation']=ddir
                    else:
                        for log in m['crawllog']:
                            print log
                            if log['status'] == 200:
                                log['disklocation']=ddir
                    mfiles.append(m)
        with open(os.path.join(dir, "meta.json"), 'w') as outfile:
            json.dump(mfiles, outfile, default=date_handler)
        
        
   



    #dbm.updateTimeInSnapshotStatusTable(sn=sn, key="fetch_end")