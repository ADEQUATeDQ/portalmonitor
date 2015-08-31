'''
Created on Aug 21, 2015

@author: jumbrich
'''
from odpw.reporting.reporters import CSVReporter, UIReporter, CLIReporter,\
    Reporter
import pandas as pd

class TimePeriodReporter(Reporter, CLIReporter, UIReporter, CSVReporter):
    
    def __init__(self, analyser):
        self.a = analyser
        super(TimePeriodReporter, self).__init__()
        
    
    def getDataFrame(self):
        
        mind=self.a.getResult()['min']
        maxd=self.a.getResult()['max']
        
        d={}
        if mind:
            d['min']=mind.isoformat()
        if maxd:
              d['max']=maxd.isoformat()
        if mind and maxd:
            delta=(maxd-mind)
        
            d['delta_sec']= delta.total_seconds()
        if self.df is None:
            self.df = pd.DataFrame(d.items())
        return self.df
    
    def uireport(self):
        mind=self.a.getResult()['min']
        maxd=self.a.getResult()['max']
        
        d={'min':None, 'max':None,'delta_sec':None }
        if mind:
            d['min']=mind.strftime('%Y-%m-%d')
        if maxd:
              d['max']=maxd.strftime('%Y-%m-%d')
        if mind and maxd:
            delta=(maxd-mind)
            d['delta_sec']= delta.total_seconds()
        
        return {self.name():d}
    
class FetchTimePeriodReporter(TimePeriodReporter):
    pass
    
class HeadTimePeriodReporter(TimePeriodReporter):
    pass