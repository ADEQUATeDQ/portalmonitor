'''
Created on Aug 27, 2015

@author: jumbrich
'''
import pandas as pd

from odpw.reporting.reporters.reporters import UIReporter, CLIReporter, Reporter


class RetrievabilityReporter(Reporter, UIReporter, CLIReporter):
    
    
    def __init__(self, analyser):
        super(RetrievabilityReporter,self).__init__(analyser)
        
        
        
    def getDataFrame(self):
        if self.df is None:
            self.df= pd.DataFrame(self.a.getResult().items())

    def uireport(self):
        res = self.a.getResult()
        
        hist = res[self.a.name()]['avgP']['hist']
        res[self.a.name()]['avgP']['histui']=[]
        t=sum(hist)
        i=0
        for h in hist:
            res[self.a.name()]['avgP']['histui'].append({ 'bin':i,'value':h, 'perc': h/(t*1.0)
                                                          })
            i+=1
        
        return {self.name():res}

class DatasetRetrievabilityReporter(RetrievabilityReporter):
    pass
        

class ResourceRetrievabilityReporter(RetrievabilityReporter):
    pass