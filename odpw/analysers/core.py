'''
Created on Jul 27, 2015

@author: jumbrich
'''
from odpw.analysers import Analyser
from _collections import defaultdict
import numpy as np
from odpw.utils.dataset_converter import dict_to_dcat
import json
from pybloom import ScalableBloomFilter

class HistogramAnalyser(Analyser):
    
    def __init__(self, funct=None, **nphistparams):
        self.list=[]
        self.funct = funct
        self.nphistparams=nphistparams
        
    def analyse_generic(self, element):
        if self.funct is not None:
            self.list.append(self.funct(element))
        else:
            self.list.append(element)
            
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
        self.bloom=None
        if withDistinct:
            self.bloom=ScalableBloomFilter()
            self.distinct=0
            self.set=set([])
    
    def analyse_generic(self, element):
        self.count+=1
        
        #TODO prob datastrucutre for distinct
        if self.bloom is not None and element not in self.bloom:
            self.distinct+=1
            self.bloom.add(element)
            
        #else:
            #print "seen", element
            
    def getResult(self):
        res= {'count':self.count}
        if self.bloom is not None:
            res['distinct']=self.distinct
        return res
    
class DBAnalyser(Analyser):
    
    def __init__(self):
        self.rows=[]
        self.columns=None
        
        
    def done(self):
        pass    
    def analyse_generic(self, element):
        self.rows.append(dict(element))
    
    def getResult(self):
        return {'rows':self.rows} 

class DCATConverter(Analyser):
    
    def __init__(self, Portal):
        self.Portal=Portal
    
    def analyse_Dataset(self, dataset):
        if dataset.data: 
            #import pprint
            #print "----"
            #pprint.pprint(dataset.data)
            #print ">DCAT"
            dcat_dict = dict_to_dcat(dataset.data, self.Portal)
            #pprint.pprint(dcat_dict)
            dataset.dcat=dcat_dict
        
