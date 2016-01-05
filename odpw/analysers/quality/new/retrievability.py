'''
Created on Aug 27, 2015

@author: jumbrich
'''
from odpw.analysers import Analyser
from odpw.analysers.statuscodes import ResourceStatusCode, DatasetStatusCode
from odpw.analysers.core import HistogramAnalyser



class Retrievability(HistogramAnalyser):
    def __init__(self, analyser, **kwargs):
        super(Retrievability, self).__init__(**kwargs)
        self.a=analyser
    
    def getResult(self):
        
        r = self.a.getResult()
        available=0.0
        t = sum(r.values())*1.0
        for k,v in r.items():
            available += v if str(k).startswith("2") else 0.0;
        
        sures= super(Retrievability,self).getResult()
        q=sum(self.list) / float(len(self.list))  if len(self.list)>0 else 0
        res ={ self.name(): 
                {
                    'total': { 'qrd': available/t if t>0 else 0, 'total':t, '2xx':available },
                    'avgP': { 
                             'qrd': q, 'total':len(self.list),
                             'hist':sures['hist'].tolist(),'bin_edges':sures['bin_edges'].tolist()
                             }
                    }
                }
        return res

    def analyse_PortalMetaData(self, pmd):
        res = self.a.analyse_PortalMetaData(pmd)
        
        available=0.0
        t = sum(res.values())*1.0
        for k,v in res.items():
            available+= v if str(k).startswith("2") else 0.0;
        
        q=available/t if t>0 else 0.0
    
        self.analyse_generic(q)
        
        
        
    def update_PortalMetaData(self, pmd):
        if pmd.qa_stats is None:
            pmd.qa_stats={}
        pmd.qa_stats[self.name()]=self.getResult()
        
class ResourceRetrievability(Retrievability):
    
    def __init__(self, analyser):
        super(ResourceRetrievability,self).__init__(analyser, funct=None, range=[0,1])
        
class DatasetRetrievability(Retrievability):
    def __init__(self, analyser):
        super(DatasetRetrievability,self).__init__(analyser, funct=None, range=[0,1])


# DCAT retrievability analyser, works with a status code distribution analylser
class RetrieveMetric(Analyser):
    def __init__(self, status_code_analyser):
        self.a = status_code_analyser
        self.total = 0
        self.count = 0

    def getResult(self):
        dist = self.a.getResult()
        # no values available
        if len(dist) == 0 or (len(dist) == 1 and -1 in dist):
            return None

        two = 0.0
        rest = 0.0
        for x in dist:
            if x != -1:
                if 200 <= x < 300:
                    two += dist[x]
                rest += dist[x]
        if rest == 0.0:
            return None
        self.total = rest
        self.count = two
        return two/rest

    def update_PortalMetaData(self, pmd):
        if pmd.qa_stats:
            pmd.qa_stats[self.name()] = self.getResult()
            pmd.qa_stats[self.name() + '_hist'] = {1: self.count, 0: self.total - self.count}


class ResRetrieveMetric(RetrieveMetric):
    def name(self):
        return 'ReRe'


class DSRetrieveMetric(RetrieveMetric):
    def name(self):
        return 'ReDa'

