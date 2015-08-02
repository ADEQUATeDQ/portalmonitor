'''
Created on Jul 27, 2015

@author: jumbrich
'''
from odpw.analysers import Analyser
from _collections import defaultdict
import pandas as pd
import numpy as np

class HistogramAnalyser(Analyser):
    
    def __init__(self, **nphistparams):
        self.list=[]
        self.nphistparams=nphistparams
    def analyse_generic(self, element):
        if self.funct is not None:
            self.append(self.funct(element))
        else:
            self.append(element)    
            
    def getRawResult(self):
        return np.array(self.list)
        
    def getResult(self):
        
        hist, bin_edges = np.histogram(np.array(self.list), **self.nphistparams)
        return {'hist':hist, 'bin_edges':bin_edges}


class ElementCountAnalyser(Analyser):
    """
    Provides a count per distinct element
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
    

class StatusCodeAnalyser(ElementCountAnalyser):
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
    
class DistinctElementCount(Analyser):
    def __init__(self, withDistinct=None):
        super(DistinctElementCount, self).__init__()
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
    
class DBAnalyser(object):
    
    def __init__(self, func, **param):
        self.func=func
        self.rows=[]
        if param:
            self.param=param
        else:
            self.param={}
        
        self.columns=None
        
    def analyse(self):
        
        res = self.func(**self.param)
        self.columns=res.keys()
        for r in res:
            self.rows.append(r)
    
    def getResult(self):
        return {'columns':self.columns, 'rows':self.rows} 
    
    
if __name__ == '__main__':
    def f1(test=None):
        print test
    
    
    def calling(func, **param):
        if param:
            p=param
        else:
            p={}
        func(**p)
        
    d = DBAnalyser(f1)
    d.analyse()
        
    