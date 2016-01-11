'''
Created on Aug 21, 2015

@author: jumbrich
'''
from odpw.analysers import Analyser
import dateutil.parser
import pytz
from odpw.utils.util import ErrorHandler
import structlog
from odpw.utils import fetch_stats
log =structlog.get_logger()


class TimeSpanAnalyser(Analyser):
    def __init__(self):
        self.deltas=[]
        
        
    def add_delta(self, size, delta):
        self.deltas.append( {'size':size, 'delta':delta})
    
    def getResult(self):
        return self.deltas

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
   
   
class FetchProcessAnalyser(Analyser):
    
    def __init__(self, snapshot):
        self.start=None
        self.dict={}
        self.sn=snapshot
        self.total=0
        self.processed=0
        self.error=0
        self.ds={'accessed':0,
                 'added_accessed':0,
                 'added_mis_av':0,
                 'mis_av':0
                 }
        self.mis_portals=0
        
        
        
            
    def analyse_PortalMetaData(self, pmd):
        if pmd.fetch_stats:
            #sum up all the ds information
            for k in self.ds:
                self.ds[k] += pmd.fetch_stats.get(k,0)
            if  pmd.fetch_stats.get('mis_av',0) >0:
                
                self.mis_portals +=1
                print pmd.snapshot
                print pmd.portal_id
                
            
                
            self.total += 1
            if 'fetch_start' in pmd.fetch_stats and  'fetch_end' in pmd.fetch_stats:
                self.processed += 1
                
                start= dateutil.parser.parse(pmd.fetch_stats['fetch_start'])
                if self.start is None or self.start > start:
                    self.start=start
                    
                end = dateutil.parser.parse(pmd.fetch_stats['fetch_end'])
                delta=(end-start)
                
                if start not in self.dict:
                    self.dict[start]=[]
                self.dict[start].append( delta.total_seconds() )
            elif all( pmd.fetch_stats.get(k) for k in ['fetch_start', 'exception']):
                self.error += 1 
    
    def getBin(self, seconds):
        bin = int(seconds/(60)) # convert to minutes
        
        if bin == 1:
            return bin
        
        elif bin <30 :
            return 30
        
        elif bin <60:
            return 60
        
        else:
            bin = int(bin/60)
            return bin*60
    
    
    def getResult(self):
        
        results={"sn":self.sn, "total":self.total, "fetched":self.processed,  "error":self.error}
        
        data=[]
        for start, durations in self.dict.items():
            for dur in durations:
                delta=( start-self.start).total_seconds()+dur
                data.append(delta)
        
        results['durations']=data
        
        for k in self.ds:
            results[k] = self.ds[k]
        results['mis_portals'] = self.mis_portals 
        return results
        
class HeadProcessAnalyser(FetchProcessAnalyser):
    
    def analyse_PortalMetaData(self, pmd):
        if pmd.fetch_stats:
            self.total += 1
            
            if 'first_lookup' in pmd.fetch_stats and  'last_lookup' in pmd.fetch_stats:
                self.processed += 1
                
                start= dateutil.parser.parse(pmd.fetch_stats['first_lookup'])
                if self.start is None or self.start > start:
                    self.start=start
                    
                end = dateutil.parser.parse(pmd.fetch_stats['last_lookup'])
                delta=(end-start)
                
                if start not in self.dict:
                    self.dict[start]=[]
                self.dict[start].append( delta.total_seconds() )
            elif 'first_lookup' in pmd.fetch_stats and  'last_lookup' not in pmd.fetch_stats:
                self.error += 1 

        
class FetchTimeSpanAnalyser(TimeSpanAnalyser):
    def analyse_PortalMetaData(self, pmd):
        if pmd.fetch_stats:
            if 'fetch_start' in pmd.fetch_stats and  'fetch_end' in pmd.fetch_stats:
                start= dateutil.parser.parse(pmd.fetch_stats['fetch_start'])
                end = dateutil.parser.parse(pmd.fetch_stats['fetch_end'])
                delta=(end-start)
                self.add_delta(pmd.datasets, delta.total_seconds())
                

class FetchPeriod(PeriodAnalyser):
    def analyse_PortalMetaData(self, pmd):
        if pmd.fetch_stats:
            for f in ['fetch_start', 'fetch_end']:
                if f in pmd.fetch_stats:
                    self.add_time(dateutil.parser.parse(pmd.fetch_stats[f]))
    
    
    def analyse_dict(self, d):
        if 'fetch_stats' in d:
            for f in ['fetch_start', 'fetch_end']:
                if f in d['fetch_stats']:
                    self.add_time(dateutil.parser.parse(d['fetch_stats'][f]))
        

class HeadTimeSpanAnalyser(TimeSpanAnalyser):
    def analyse_PortalMetaData(self, pmd):
        if pmd.fetch_stats:
            if 'first_lookup' in pmd.fetch_stats and  'last_lookup' in pmd.fetch_stats:
                start= dateutil.parser.parse(pmd.fetch_stats['first_lookup'])
                end = dateutil.parser.parse(pmd.fetch_stats['last_lookup'])
                delta=(end-start)
                self.add_delta(pmd.resources, delta.total_seconds())

                
                
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