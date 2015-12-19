'''
Created on Aug 27, 2015

@author: jumbrich
'''
from odpw.reporting.reporters import Reporter, UIReporter, CLIReporter,\
    CSVReporter, addPercentageCol, DFtoListDict

import pandas as pd
import numpy as np

class StatusCodeReporter(Reporter,UIReporter, CLIReporter, CSVReporter):
    
    dist={
        '2':'ok',
        '3':'redirect-loop (3xx)',
        '4':'offline (4xx)',
        '5':'server-error (5xx)',
        '6':'other-error',
        '7':'connection-error',
        '8':'value-error',
        '9':'uri-error',
        '-':'unknown',        
    }
    
    
    def __init__(self, analyser):
        super(StatusCodeReporter,self).__init__(analyser)
        
        
    def getDataFrame(self):
        
        
        if self.df is None:
            d=self.a.getResult()
            res = []
            
            for k,v in d.items():
                res.append({'status':k, 'count':v, 'pre':str(k)[0],'label':self.dist[str(k)[0]]})
            self.df= pd.DataFrame(res)
        
        return self.df
            
    def uireport(self):
        
        df = self.getDataFrame()
        #total = df['count'].sum()
        
        print 'df'
        print df
        dfpref=df.groupby("pre",as_index=False)
        dfpref= dfpref.aggregate(np.sum)
        
        r = []
        for k,v in self.dist.items():
            r.append({ 'count':0, 'pre':k,'label':v}) 
        d= pd.DataFrame(r)
        
        d['count'] = dfpref.set_index(['pre'])['count'].combine_first(d.set_index(['pre'])['count']).values
        
        #d1=pd.concat([d,dfpref])
        #d.update(dfpref)
        
        
        
        return { self.name():DFtoListDict(addPercentageCol(df)),
                self.name()+"_chart":DFtoListDict(addPercentageCol(d))
                }
        
         
class DatasetStatusCodeReporter(StatusCodeReporter):
    pass
class ResourcesStatusCodeReporter(StatusCodeReporter):
    pass   
        