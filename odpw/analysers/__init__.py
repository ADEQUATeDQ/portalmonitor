from odpw.db.models import Portal, PortalMetaData
import pandas
from odpw.timer import Timer
from odpw.util import timer
import types


__author__ = 'jumbrich'

from _collections import defaultdict
import time
import numpy as np

class Analyser:
    def analyse(self, element): pass
    def getResult(self): pass
    def getDataFrame(self): pass
    @classmethod
    def name(cls): return cls.__name__
    def done(self): pass





class CountAnalser(object):
    """
    Analyser which provides a count per distinct element
    """
    def __init__(self):
        self.dist=defaultdict(int)
    def add(self, value, count=1): 
        self.dist[value]+=count
    def getDist(self):
        return dict(self.dist)
    

class StatusCodeAnalyser(CountAnalser):
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

class PortalSoftwareDistAnalyser(Analyser,CountAnalser):
    def analyse(self, portal):
        #if not isinstance(portal, Portal):
        #    return
        self.add(portal['software'])
    def getResult(self):
        return self.getDist()
    def getDataFrame(self):
        return pandas.DataFrame([[col1,col2] for col1,col2 in self.getDist().items()], columns=['software', 'count'])




class PortalCountryDistAnalyser(Analyser,CountAnalser):
    def analyse(self, portal):
        if not isinstance(portal, Portal):
            return
        self.add(portal.country)
    def getResult(self):
        return self.getDist()
    def getDataFrame(self):
        return pandas.DataFrame([[col1,col2] for col1,col2 in self.getDist().items()], columns=['country', 'count'])
        

class PortalStatusAnalyser(StatusCodeAnalyser, Analyser):
    def analyse(self, portal):
        #if not isinstance(portal, Portal):
        #    return
        self.add(portal['status'])
        
    def getResult(self):
        return self.getDist()

    def getDataFrame(self):
        return pandas.DataFrame([[status,val['count'],val['label']] for status, val in self.getDist().items() ], columns=['status_prefix', 'count', 'label'])


class StatusAnalyserPMD(StatusCodeAnalyser, Analyser):
    def analyse(self, pmd):
        if not isinstance(pmd, PortalMetaData):
            return
        
        if pmd.fetch_stats and "portal_status" in pmd.fetch_stats:
            self.add(pmd.fetch_stats['portal_status'])
        else:
                self.add(999)
        
    def getResult(self):
        return self.getDist()

    def getDataFrame(self):
        data=[]
        for snapshot, statusDist in self.getDist().items():
            for status , val in statusDist.items():
                data.append([snapshot,status,val['count'],val['label']])
            
        return pandas.DataFrame(data, columns=['snapshot','status_prefix', 'count', 'label'])


class DatasetDistAnalyserPMD(Analyser): 
    
    def __init__(self):
        self.total=[]
        self.processed=[]
    
    def analyse(self, pmd):
        if not isinstance(pmd, PortalMetaData):
            return
        
        if pmd.datasets>0:
            self.total.append(pmd.datasets)
        else:
            self.total.append(0)
        if pmd.fetch_stats and "datasets" in pmd.fetch_stats and pmd.fetch_stats['datasets']!=-1:
            self.processed.append(pmd.fetch_stats['datasets'])
        else:
            self.processed.append(0)

    def getResult(self):
        return {'total':self.total, 'processed':self.processed}

class ResourceDistAnalyserPMD(Analyser): 
    
    def __init__(self):
        self.total=[]
        self.processed=[]
    
    def analyse(self, pmd):
        if not isinstance(pmd, PortalMetaData):
            return
        
        if pmd.resources>0:
            self.total.append(pmd.resources)
        else:
            self.total.append(0)
        if pmd.res_stats and "total" in pmd.res_stats and pmd.res_stats['total']!=-1:
            self.processed.append(pmd.res_stats['total'])
        else:
            self.processed.append(0)

    def getResult(self):
        return {'total':self.total, 'processed':self.processed}

class AnalyseEngine(object):
    def __init__(self):
        self.analysers = {}
        
    def add(self, analyser):
        self.analysers[analyser.name()] = analyser

    def analyse(self, element):
        with Timer(key="analyse") as t:
            for c in self.analysers.itervalues():
                c.analyse(element)
    
    def done(self):
        for c in self.analysers.itervalues():
            c.done()
        #self.end=time.time()
        #print 'AnalyseEngine elapsed time: %s (%f ms)' % (timer(self.end-self.start),(self.end-self.start)*1000)
    
    def process_all(self, iterable):
        self.start= time.time()
        for c in iterable:
            self.analyse(c)
        self.done()
            
    def getAnalyser(self, analyser):
        
        if isinstance(analyser, (types.TypeType, types.ClassType)) and  issubclass(analyser, Analyser):
            return self.analysers[analyser.name()]
        elif isinstance(analyser, analyser):
            return self.analysers[analyser.name()]