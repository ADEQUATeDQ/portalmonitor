'''
Created on Aug 12, 2015

@author: jumbrich
'''
from odpw.analysers import Analyser
from _collections import defaultdict
from odpw.analysers.datasetlife import DatasetLifeStatsAnalyser
from odpw.analysers.core import DBAnalyser


class SystemEvolutionAnalyser(DBAnalyser):
    pass


class EvolutionCountAnalyser(Analyser):
    
    def __init__(self):
        self._evolv={}
        self.keys=set([])
        
    def add(self, snapshot, key, value):
        sndict = self._evolv.get(snapshot, defaultdict(int))
        sndict[key]+=value
        self._evolv[snapshot]=sndict
        self.keys.add(key)

    def getResult(self):
        res={}
        for sn, ddict in self._evolv.items():
            res[sn]= { k: ddict.get(k) for k in self.keys}
        return res

class DatasetEvolution(EvolutionCountAnalyser):
    def __init__(self):
        super(DatasetEvolution, self).__init__()
    
    def analyse_PortalMetaData(self, pmd):
        if pmd.datasets:
            self.add(pmd.snapshot, 'datasets', pmd.datasets)
        #if pmd.fetch_stats:
        #    for i in DatasetLifeStatsAnalyser.keys:
        #        self.add(pmd.snapshot, i, pmd.fetch_stats.get(i,0))
        #else:
        #    self.add(pmd.snapshot,'no_stats', 1)
        
        
        
    
class ResourceEvolution(EvolutionCountAnalyser):
    def __init__(self):
        super(ResourceEvolution, self).__init__()
    def analyse_PortalMetaData(self, pmd):
        if pmd.resources:
            self.add(pmd.snapshot, 'resources', pmd.resources)
        
        #self.add(pmd.snapshot, 'resources',pmd.resources)

class ResourceAnalysedEvolution(EvolutionCountAnalyser):
    def analyse_PortalMetaData(self, pmd):
        count=0
        if pmd.res_stats and 'status' in pmd.res_stats:
            count = sum(pmd.res_stats['status'].values())
        self.add(pmd.snapshot, 'resources_analysed',count)

        
class SystemSoftwareEvolution(EvolutionCountAnalyser):
    
    def __init__(self, portalSoftware):
        self.portalSoftware= portalSoftware
        super(SystemSoftwareEvolution, self).__init__()
        
    def analyse_PortalMetaData(self, pmd):
        self.add(pmd.snapshot, self.portalSoftware[pmd.portal_id], 1)



class DatasetDCATMetricsEvolution(EvolutionCountAnalyser):
    def __init__(self, metrics):
        super(DatasetDCATMetricsEvolution, self).__init__()
        self.metrics = metrics

    def analyse_PortalMetaData(self, pmd):
        if pmd.qa_stats:
            for m in self.metrics:
                self.add(pmd.snapshot, m, pmd.qa_stats.get(m, 0))
        else:
            self.add(pmd.snapshot, 'no_qa_stats', 1)

class PMDCountEvolution(EvolutionCountAnalyser):
    def analyse_PortalMetaData(self, pmd):
        if pmd.datasets:
            self.add(pmd.snapshot, 'datasets', pmd.datasets)
        else:
            self.add(pmd.snapshot,'no_datasets', 1)

        if pmd.resources:
            self.add(pmd.snapshot, 'resources', pmd.resources)
        else:
            self.add(pmd.snapshot,'no_resources', 1)