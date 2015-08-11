'''
Created on Aug 10, 2015

@author: jumbrich
'''
from odpw.analysers.core import ElementCountAnalyser


class DatasetStatusCount(ElementCountAnalyser):
#    __metaclass__ = AnalyserFactory
    def analyse_Dataset(self, dataset):
        self.add(dataset.status)

    def update_PortalMetaData(self, pmd):
        if not pmd.fetch_stats:
            pmd.fetch_stats = {}
        pmd.fetch_stats['respCodes'] = self.getResult()
        
        
class ResourceStatusCode(ElementCountAnalyser):
    def analyse_Resource(self, res):
        self.add(res.status)
    
    def update_PortalMetaData(self, pmd):
        if not pmd.res_stats:
            pmd.res_stats = {}
        pmd.res_stats['respCodes'] = self.getResult()