'''
Created on Aug 12, 2015

@author: jumbrich
'''
from odpw.reporting.reporters import Reporter, UIReporter, CLIReporter,\
    DFtoListDict, CSVReporter
import pandas as pd


class EvolutionReporter(Reporter, UIReporter, CLIReporter, CSVReporter):
    
    def __init__(self, analyser):
        super(EvolutionReporter, self).__init__()
        self.a=analyser
    
    def getDataFrame(self):
        if  self.df is None:
            res=[]
            print self.a.getResult()
            for sn, dkv in  self.a.getResult().items():
                d={'snapshot':sn}
                for k,v in dkv.items():
                    d[k]=v
                res.append(d) 
            self.df= pd.DataFrame(res)
        return self.df
    
class DatasetEvolutionReporter(EvolutionReporter):
    pass
class ResourcesEvolutionReporter(EvolutionReporter):
    pass
class ResourceAnalyseReporter(EvolutionReporter):
    pass 
class SystemSoftwareEvolutionReporter(EvolutionReporter):
    pass