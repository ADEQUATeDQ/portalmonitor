'''
Created on Aug 12, 2015

@author: jumbrich
'''
from odpw.reporting.reporters import Reporter, UIReporter, CLIReporter,\
    DFtoListDict
import pandas as pd


class EvolutionReporter(Reporter, UIReporter, CLIReporter):
    
    def __init__(self, analyser):
        super(EvolutionReporter, self).__init__()
        self.a=analyser
    
    def getDataFrame(self):
        if  self.df is None:
            self.df= pd.DataFrame(self.a.getResult().items(), columns=['snapshot','datasets'])
        return self.df
    
    def uireport(self):
        return {self.name():DFtoListDict(self.getDataFrame())}

    def clireport(self):
        df = self.getDataFrame()
        print df
        