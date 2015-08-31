'''
Created on Aug 27, 2015

@author: jumbrich
'''
from odpw.reporting.reporters import UIReporter, CLIReporter, Reporter

import pandas as pd


class RetrievabilityReporter(Reporter, UIReporter, CLIReporter):
    
    
    def __init__(self, analyser):
        super(RetrievabilityReporter,self).__init__()
        self.a=analyser
        
        
    def getDataFrame(self):
        if self.df is None:
            self.df= pd.DataFrame(self.a.getResult().items())

    def uireport(self):
        return {self.name():self.a.getResult()}

class DatasetRetrievabilityReporter(RetrievabilityReporter):
    pass
        

class ResourceRetrievabilityReporter(RetrievabilityReporter):
    pass