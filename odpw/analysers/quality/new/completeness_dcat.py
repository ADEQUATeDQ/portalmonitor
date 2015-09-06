'''
Created on Aug 18, 2015

@author: jumbrich


Existence of crucial meta data information
'''
from odpw.analysers import Analyser
from odpw.utils.dataset_converter import DCT, DCAT

import datetime
from odpw.analysers.quality.new.existence_dcat import getAllProvAnalyser,\
    getAllDescriptiveAnalyser
## Provenance 



class CompletenessDCATAnalyser(Analyser):
    
    def __init__(self, checkers):
        self.checkers=checkers
        self.count=0
        self.sum=0
    def analyse_Dataset(self, dataset):
        f=0.0
        t=0.0
        for c in self.checkers:
            v=c.analyse_Dataset(dataset)
            if v:
                t+=1.0
            else:
                f+=1.0
            
        self.count+=1
        self.sum+= t/(f+t)
    
    def getResult(self):
        c= self.sum/self.count if self.count>0 else 0
        return {"C":c, 'count':self.count}
            

class DescriptiveDCATAnalyser(CompletenessDCATAnalyser):
    def __init__(self):
        super(DescriptiveDCATAnalyser,self).__init__(getAllDescriptiveAnalyser())
        
