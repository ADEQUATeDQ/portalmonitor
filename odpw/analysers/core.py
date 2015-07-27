'''
Created on Jul 27, 2015

@author: jumbrich
'''
from odpw.analysers import Analyser
from _collections import defaultdict
import pandas as pd


class CountAnalyser(Analyser):
    """
    Analyser which provides a count per distinct element
    """
    def __init__(self, funct=None):
        self.dist=defaultdict(int)
        self.funct=funct
    
    def analyse_generic(self, element):
        if self.funct is not None:
            self.add(self.funct(element))
        else:
            self.add((element))
    
    def add(self, value, count=1): 
        self.dist[value]+=count
    
    def getDist(self):
        return dict(self.dist)
    
    def getResult(self):
        return self.getDist()
    

class StatusCodeAnalyser(CountAnalyser):
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
                'total':'total'
    }
    
    def add(self, value, count=1):
        status = sstr=str(value)[0]
        super(StatusCodeAnalyser,self).add(status,count=count)
        super(StatusCodeAnalyser,self).add('total',count=count)

    def getDist(self):
        d={}
        for k,v in dict(self.dist).iteritems():
            d[k]={'count':v, 'label': self.__class__.dist[k]}
        return d
    
class ElementCount(Analyser):
    def __init__(self, withDistinct=None):
        super(ElementCount, self).__init__()
        self.count=0
        self.set=None
        if withDistinct:
            self.set=set([])
    
    def analyse_generic(self, element):
        self.count+=1
        
        #TODO prob datastrucutre for distinct
        if self.set is not None and element not in self.set:
            self.set.add(element)
            
    def getResult(self):
        res= {'count':self.count}
        if self.set is not None:
            res['distinct']=len(self.set)
        return res