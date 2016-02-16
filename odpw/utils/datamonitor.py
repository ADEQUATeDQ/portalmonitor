
import os
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
    return 'DMInfo'

def setupCLI(pa):
    pa.add_argument("-f","--file",  help='file containing csv urls', dest='file')
    pa.add_argument("-p","--pickle",  help='pickle file containing csv urls', dest='pickle')
    pa.add_argument("-s","--size",  help='maximum size in KB if in header', dest='size', type=int, default=-1)
    pa.add_argument('-o','--out',type=str, dest='out' , help="the out directory for the list of urls (and downloads)")

def cli(args, dbm):

    experiment = 'csvengine'
    
    dm_dbm = DMManager(db='datamonitor', host="datamonitor-data.ai.wu.ac.at", port=5432, password='d4tamonitor', user='datamonitor')

    urls=None
    if args.file:
        urls = {}
        with open(args.file) as f:
            for u in f:
                url = u.strip()
                try:
                    urls[url]={}
                except Exception, e:
                    print e
    if args.pickle:
        import pickle
        with open( args.pickle, "rb" ) as p: 
            urls = pickle.load(p)

    files={}
    for k,v in urls.iteritems():
        print "checking", k
        res = dm_dbm.getLatestURLInfo(k)
        for r in res:
            if '2015' in r['disklocation']:
                v['file']=r['disklocation']
                v['size']=r['size']
                files[k]=v
                
    
    with open(os.path.join(args.out, 'csv_urls_files.pkl'), 'wb') as f:
        pickle.dump(files, f)
        print 'Writing dict to ',f
    
            
            
        
        
        
            


    

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

