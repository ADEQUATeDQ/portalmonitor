'''
Created on Aug 12, 2015

@author: jumbrich
'''
from odpw.analysers import Analyser


class EvolutionAnalyser(Analyser):
    
    def __init__(self):
        self._evolv={}
        
    def add(self, snapshot, value):
        self._evolv[snapshot]=value

    def getResult(self):
        return self._evolv


class DatasetEvolution(EvolutionAnalyser):
    
    def analyse_PortalMetaData(self, pmd):
        self.add(pmd.snapshot, pmd.datasets)
    
class ResourceEvolution(EvolutionAnalyser):
    def analyse_PortalMetaData(self, pmd):
        self.add(pmd.snapshot, pmd.resources)