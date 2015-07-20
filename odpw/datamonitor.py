__author__ = 'jumbrich'


import sys
from odpw.db.models import Portal
from odpw.db.POSTGRESManager import PostGRESManager
from odpw.util import ErrorHandler as eh, getSnapshot
from datetime import date, datetime, timedelta

import logging
log = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()

def name():
    return 'DM'

def setupCLI(pa):
    pa.add_argument('-s','--schedule',action='store_true', dest='schedule')
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')

def cli(args,dbm):
    if args.schedule:
        sn = getSnapshot(args)
        if not sn:
            return
        
        today = datetime.now()
        start = today + timedelta( (6-today.weekday()) % 7 )
    
        start= start.replace(hour=0, minute=0, second=0)
        end=start+timedelta(5)
        
        log.info("Scheduling URIs", start=start, end=end)
        
        experiment='odwu'
        
        nextCrawl=start
        
        dm_dbm= PostGRESManager(db='datamonitor', host="137.208.51.23", port=5432, password=None, user='postgres')
        
        #get list of URLs for the snapshot 
        for res in dbm.selectQuery("Select * from resources WHERE snapshot='"+sn+"'"):
            #print str(res['status'])[0]
            uri=res['url']
            
            if str(res['status'])[0] == '2' or str(res['status'])[0] =='5': 
                print res['status'],res['url'], nextCrawl, experiment
            
                dm_dbm.insertQuery(
                           "INSERT INTO schedule (uri,experiment,nextcrawltime, frequency) "
                           "SELECT %s,%s,%s,%s " 
                           "WHERE NOT EXISTS (SELECT 1 FROM schedule WHERE uri=%s AND experiment=%s);",
                           tuple=(uri, experiment, nextCrawl, 10800, uri, experiment)
                           )
            
                nextCrawl +=timedelta(hours=1)
                if nextCrawl>=end:
                    nextCrawl=start
        
        
        
        
        
        
        
        
        
        
       
     
    print "Numbers of Exceptions:"
    for exc, count in eh.exceptions.iteritems():
        print exc, count
        
if __name__ == '__main__':
    
    
    

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