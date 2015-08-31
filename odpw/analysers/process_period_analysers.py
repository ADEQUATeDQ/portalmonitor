'''
Created on Aug 21, 2015

@author: jumbrich
'''
from odpw.analysers import Analyser
import dateutil.parser
import pytz
from odpw.utils.util import ErrorHandler
import structlog
log =structlog.get_logger()

class PeriodAnalyser(Analyser):
    
    def __init__(self):
        self.min=None
        self.max=None
        
    def add_time(self, dt):
        if not dt.tzinfo:
            dt=pytz.utc.localize(dt)
    
        if self.min is None or self.min >dt: 
            self.min=dt
        
        if self.max is None or self.max <dt:
            self.max=dt
    
    
    def getResult(self):
        return {'min':self.min, 'max':self.max}
            
        

class FetchPeriod(PeriodAnalyser):
    def analyse_PortalMetaData(self, pmd):
        if pmd.fetch_stats:
            for f in ['fetch_start', 'fetch_end']:
                if f in pmd.fetch_stats:
                    self.add_time(dateutil.parser.parse(pmd.fetch_stats[f]))
                
class HeadPeriod(PeriodAnalyser):
    
    def analyse_Resource(self, resource):
        if resource.header:
            if 'date' in resource.header:
                date=None
                dateheader=resource.header['date']
                try:
                    date = dateutil.parser.parse(dateheader)
                except Exception as e: 
                    try:
                        span = 2
                        if dateheader.count(",") == 3:
                            words = dateheader.split(",")
                            ss=  [",".join(words[i:i+span]) for i in range(0, len(words), span)]
                            date=dateutil.parser.parse(ss[0])
                    except Exception as e:
                        ErrorHandler.handleError(log, "Dateformatexception", exception=e, dateheader=dateheader)
                if date:
                    
                    self.add_time(date)
                    
    def analyse_PortalMetaData(self, pmd):
        if pmd.res_stats:
            for f in ['first_lookup', 'last_lookup']:
                if f in pmd.res_stats:
                    self.add_time(dateutil.parser.parse(pmd.res_stats[f]))
            
    def update_PortalMetaData(self, pmd):
        if all([self.min, self.max]):
            if pmd.res_stats is None:
                pmd.res_stats={}
            pmd.res_stats['first_lookup']= self.min.isoformat()
            pmd.res_stats['last_lookup']= self.max.isoformat()