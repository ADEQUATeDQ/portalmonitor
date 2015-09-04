'''
Created on Aug 18, 2015

@author: jumbrich


Existence of crucial meta data information
'''
from odpw.analysers import Analyser
from odpw.utils.dataset_converter import DCT, DCAT

import datetime
from odpw.analysers.core import ElementCountAnalyser
from odpw.analysers.quality.new.existence_dcat import ProvCreationDCAT,\
    getAllProvAnalyser
## Provenance 

class CompletenessDCATAnalyser(Analyser):
    
    def __init__(self):
        self.checkers=getAllProvAnalyser()
        self.count=0
        self.sum=0
    def analyse_Dataset(self, dataset):
        for dcat_el in getattr(dataset,'dcat',[]):
            if '@type' in dcat_el:
                for c in dcat_el.get('@type',[]):
                    print c
                for k, v in dcat_el.items():
                    print "  -  ", k,v
                
            else:
                for k, v in dcat_el.items():
                    print "  ", k,v
        f=0.0
        t=0.0
        for c in self.checkers:
            v=c.analyse_Dataset(dataset)
            if v:
                t+=1.0
            else:
                f+=1.0
            print c.name(),v
            
        self.count+=1
        self.sum+= t/(f+t)
    def getResult(self):
        c= self.sum/self.count if self.count>0 else 0
        return {"C":c, 'count':self.count}
            
