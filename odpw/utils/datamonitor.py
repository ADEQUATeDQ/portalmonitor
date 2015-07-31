from odpw.db.dbm import DMManager
__author__ = 'jumbrich'


import sys
from odpw.db.models import Portal

from odpw.utils.util import ErrorHandler as eh, getSnapshot, progressIndicator
from datetime import date, datetime, timedelta

import logging
log = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()


def help():
    return "INsert URIs into the datamonitor"
def name():
    return 'DM'

def setupCLI(pa):
    pa.add_argument('-s','--schedule',action='store_true', dest='schedule')
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')

def cli(args,dbm):
    
        sn = getSnapshot(args)
        if not sn:
            return
        
        today = datetime.now()
        start = today + timedelta((6 - today.weekday()) % 7)
        start = start.replace(hour=0, minute=0, second=0)
        end = start + timedelta(5)
        log.info("Scheduling URIs", start=start, end=end)
        
        experiment = 'odwu'
        
        dm_dbm = DMManager(db='datamonitor', host="137.208.51.23", port=5432, password='p0stgres', user='postgres')
        
        urls={}
        # get list of URLs for the snapshot 
        #for res in dbm.getResources(snapshot=sn, status=-1):
        #    # print str(res['status'])[0]
        #    urls[res['url']]={'experiment':experiment, 'freq':10800}
        
        nct = today - timedelta(1)
        print nct
        for resS in dm_dbm.getOldSchedule(nct):
            urls[ resS['uri'] ]={'experiment':resS['experiment'], 'freq':resS['frequency']}
            
        t = len(urls)
        steps = t/10
        
        print t
        
        nextCrawl = start
        c=0
        for url in urls:
            c+=1
            dm_dbm.upsert(url, urls[url]['experiment'], nextCrawl, urls[url]['freq'])
            
            if c%steps==0:
                progressIndicator(c, t)
                
            nextCrawl += timedelta(hours=1)
            if nextCrawl >= end:
                nextCrawl = start
        
        
if __name__ == '__main__':
    
    dm_dbm = DMManager(db='datamonitor', host="137.208.51.23", port=5432, password='p0stgres', user='postgres')
    
    dm_dbm.upsert('http://example', 'text', '','')
    

    def datespan(startDate, endDate, delta=timedelta(days=1)):
        currentDate = startDate
        
        
        while currentDate < endDate:
            yield currentDate
            currentDate += delta
    
    today = datetime.now()
    mon = today + timedelta( (6-today.weekday()) % 7 )
    
    print mon
    mon= mon.replace(hour=0, minute=0, second=0)
    end=mon+timedelta(7)
    
    
    
#     dm_dbm= PostGRESManager(db='datamonitor', host="137.208.51.23", port=5432, password=None, user='postgres')
#     for res in dm_dbm.selectQuery("Select * from schedule Limit 10"):
#         print res
#         
#     dm_dbm= PostGRESManager(db='portalwatch', host="137.208.51.23", port=5433, password='0pwu', user='opwu')
#     for res in dm_dbm.selectQuery("Select * from resources WHERE snapshot='2015-25'Limit 10"):
#         print res['status'],res['url']
