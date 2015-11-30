from odpw.db.dbm import DMManager
__author__ = 'jumbrich'


import sys
from odpw.db.models import Portal
import validators
from odpw.utils.util import ErrorHandler as eh, getSnapshot, progressIndicator
from datetime import date, datetime, timedelta

import logging
log = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()


def help():
    return "Insert URIs into the datamonitor"
def name():
    return 'DM'

def setupCLI(pa):
    pa.add_argument("-f","--file",  help='file containing csv urls', dest='file')
    pa.add_argument("-d","--crawldate",  help='next crawl date', dest='crawldate')

def cli(args, dbm):

    experiment = 'csvengine'
    if not args.crawldate:
        nextCrawl = datetime(year=2015, month=11, day=26, hour=12)
        print 'Crawl time', nextCrawl
    else:
        nextCrawl = datetime.strptime(args.crawldate, '%Y-%m-%dT%H:%M:%S')

    dm_dbm = DMManager(db='datamonitor', host="datamonitor-data.ai.wu.ac.at", port=5432, password='d4tamonitor', user='datamonitor')

    with open(args.file) as f:
        for u in f:
            url = u.strip()
            try:
                validators.url(url)
                dm_dbm.upsert(url, experiment, nextCrawl, 0)
            except Exception, e:
                print e

    #psql --host datamonitor-data.ai.wu.ac.at -U datamonitor -W datamonitor
        #sn = getSnapshot(args)
        #if not sn:
        #    return

        #today = datetime.now()
        #start = today + timedelta((6 - today.weekday()) % 7)
        #start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        #end = start + timedelta(days=5)
        #log.info("Scheduling URIs", start=start, end=end)

        
        #urls={}
        # get list of URLs for the snapshot 
        #for res in dbm.getResources(snapshot=sn, status=-1):
        #    # print str(res['status'])[0]
        #    urls[res['url']]={'experiment':experiment, 'freq':10800}
        
        #nct = today - timedelta(1)
        #print nct
        #for resS in dm_dbm.getOldSchedule(nct):
        #    urls[ resS['uri'] ]={'experiment':resS['experiment'], 'freq':resS['frequency']}
            
        #t = len(urls)
        #steps = t/10
        
        #print t
        
        #nextCrawl = start
        #c=0
        #for url in urls:
        #    c+=1
            
        #    if c%steps==0:
        #        progressIndicator(c, t)
                
        #    nextCrawl += timedelta(hours=1)
        #    if nextCrawl >= end:
        #        nextCrawl = start

