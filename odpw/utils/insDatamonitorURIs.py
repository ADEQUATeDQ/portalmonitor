from odpw.db.dbm_datamonitor import DMManager

__author__ = 'jumbrich'


import sys
from odpw.db.models import Portal
#import validators
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
    pa.add_argument("-p","--pickle",  help='pickle file containing csv urls', dest='pickle')
    pa.add_argument("-d","--crawldate",  help='next crawl date', dest='crawldate')
    pa.add_argument("-s","--size",  help='maximum size in KB if in header', dest='size', type=int, default=-1)
    pa.add_argument("-u","--update",  help='After how many seconds should the URL be crawled', dest='update', type=int, default=0)

def cli(args, dbm):

    experiment = 'csvengine'
    
    
    update= args.update
    if not args.crawldate:
        #schedule two hours from now
        today = datetime.now()
        
        today = datetime.now()
        start = today + timedelta(hours=1)
        nextCrawl=start
        #nextCrawl = nextCrawl.replace(hour=0, minute=0, second=0, microsecond=0)
        
        #start = today + timedelta((6 - today.weekday()) % 7)
        start = start.replace( minute=0, second=0, microsecond=0)
        end = start + timedelta(days=3)
        
        
        '''
        #nextCrawl= datetime(year=2016, month=01, day=21, hour=16)
        
        
        today = datetime.now()
        nextCrawl = today + timedelta(hours=2)
        nextCrawl = nextCrawl.replace(hour=0, minute=0, second=0, microsecond=0)
        
        start = today + timedelta((6 - today.weekday()) % 7)
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        
        end = start + timedelta(days=5)'''
        
        log.info("Scheduling URIs", start=start, end=end)        
    else:
        nextCrawl = datetime.strptime(args.crawldate, '%Y-%m-%dT%H:%M:%S')

    dm_dbm = DMManager(db='datamonitor', host="datamonitor-data.ai.wu.ac.at", port=5432, password='d4tamonitor', user='datamonitor')

    if args.file:
        with open(args.file) as f:
            for u in f:
                url = u.strip()
                try:
                    
                    
                    if not args.crawldate:
                        nextCrawl += timedelta(hours=1)
                        if nextCrawl >= end:
                            nextCrawl = start
                    print nextCrawl,update,url
                    dm_dbm.upsert(url, experiment, nextCrawl, update)
                except Exception, e:
                    print e
    if args.pickle:
        import pickle
        with open( args.pickle, "rb" ) as p: 
            urls = pickle.load(p)
            
            for k,v in urls.items():
                size = v.get('header',{}).get('size',-1)
                
                if  size<= args.size:
                    try:
                        
                        if not args.crawldate:
                            nextCrawl += timedelta(hours=1)
                            if nextCrawl >= end:
                                nextCrawl = start
                        print nextCrawl ,update , k
                        dm_dbm.upsert(k, experiment, nextCrawl, update)
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

