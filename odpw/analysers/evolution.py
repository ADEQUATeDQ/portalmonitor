'''
Created on Aug 12, 2015

@author: jumbrich
'''
from odpw.analysers import Analyser
from _collections import defaultdict
from odpw.analysers.datasetlife import DatasetLifeStatsAnalyser


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
    def analyse_PortalMetaData(self, pmd):
        if pmd.fetch_stats:
            for i in DatasetLifeStatsAnalyser.keys:
                self.add(pmd.snapshot, i, pmd.fetch_stats.get(i,0))
        else:
            self.add(pmd.snapshot,'no_stats', 1)
        
        
        
    
class ResourceEvolution(EvolutionCountAnalyser):
    def analyse_PortalMetaData(self, pmd):
        self.add(pmd.snapshot, 'resources',pmd.resources)

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