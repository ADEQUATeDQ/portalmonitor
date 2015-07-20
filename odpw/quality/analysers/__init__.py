from odpw.db.models import Portal, PortalMetaData
import pandas
from odpw.timer import Timer
from odpw.util import timer
import types


__author__ = 'jumbrich'

from _collections import defaultdict
import time

class Analyser:
    def analyse(self, element): pass
    def getResult(self): pass
    def getDataFrame(self): pass
    @classmethod
    def name(cls): return cls.__name__
    def done(self): pass


    
    
class MapDistribution(object):
    def __init__(self):
        self.dist={}
        
    def add(self, key, value):
        if key not in self.dist:
            self.dist[key]=defaultdict(int) 
        self.dist[key][value]+=1
    def getDist(self):
        d = {}
        for a in self.dist:
            d[a] = dict(self.dist[a])
        return d

class CountAnalser(object):
    def __init__(self):
        self.dist=defaultdict(int)
    def add(self, value): 
        self.dist[value]+=1
    def getDist(self):
        return dict(self.dist)
    

class StatusCodeDistribution(CountAnalser):
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
    
    def add(self, value):
        status = sstr=str(value)[0]
        super(StatusCodeDistribution,self).add(status)
        super(StatusCodeDistribution,self).add('total')

    def getDist(self):
        d={}
        for k,v in dict(self.dist).iteritems():
            d[k]={'count':v, 'label': self.__class__.dist[k]}
        return d

class StatusCodeMapDistribution(MapDistribution):
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

    
    def add(self, key, value):
        status = sstr=str(value)[0]
        super(StatusCodeMapDistribution, self).add(key, status)
        super(StatusCodeMapDistribution, self).add(key, 'total')
        
    def getDist(self):
        d={}
        for k,v in dict(self.dist).iteritems():
            d[k]={}
            for s,c in dict(v).iteritems():
                d[k][s]={'count':c, 'label': self.__class__.dist[s]}
        return d
    
    
class PortalSoftwareDistAnalyser(Analyser,CountAnalser):
    def analyse(self, portal):
        if not isinstance(portal, Portal):
            return
        self.add(portal.software)
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
        

class PortalStatusAnalyser(StatusCodeDistribution, Analyser):
    def analyse(self, portal):
        if not isinstance(portal, Portal):
            return
        self.add(portal.status)
        
    def getResult(self):
        return self.getDist()

    def getDataFrame(self):
        return pandas.DataFrame([[status,val['count'],val['label']] for status, val in self.getDist().items() ], columns=['status_prefix', 'count', 'label'])


class PortalMetaDataStatusAnalyser(StatusCodeMapDistribution, Analyser):
    def analyse(self, pmd):
        
        
        if not isinstance(pmd, PortalMetaData):
            return
        
        if pmd.fetch_stats and "portal_status" in pmd.fetch_stats:
            self.add(pmd.snapshot,pmd.fetch_stats['portal_status'])
        else:
                self.add(pmd.snapshot,-10)
        
    def getResult(self):
        return self.getDist()

    def getDataFrame(self):
        data=[]
        for snapshot, statusDist in self.getDist().items():
            for status , val in statusDist.items():
                data.append([snapshot,status,val['count'],val['label']])
            
        return pandas.DataFrame(data, columns=['snapshot','status_prefix', 'count', 'label'])

class PortalMetaDataFetchStatsAnalyser(Analyser,CountAnalser):
    def analyse(self, pmd):
        if not isinstance(pmd, PortalMetaData):
            return
        
        if pmd.fetch_stats and pmd.fetch_stats.get("portal_status",0)==200:
            if "fetch_end" in pmd.fetch_stats:
                self.add('processed')
            else:
                self.add('unprocessed')
    def getResult(self):
        return self.getDist()   
    
class PortalMetaDataResourceStatsAnalyser(Analyser,CountAnalser):
    def analyse(self, pmd):
        if not isinstance(pmd, PortalMetaData):
            return
        
        if pmd.fetch_stats and pmd.fetch_stats.get("portal_status",0)==200:
            if bool(pmd.res_stats['respCodes']):
                self.add('processed')
            else:
                self.add('unprocessed')
                
    def getResult(self):
        return self.getDist()  

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
        self.end=time.time()
        print 'AnalyseEngine elapsed time: %s (%f ms)' % (timer(self.end-self.start),(self.end-self.start)*1000)
    
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