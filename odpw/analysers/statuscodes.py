'''
Created on Aug 10, 2015

@author: jumbrich
'''
from odpw.analysers.core import ElementCountAnalyser


class DatasetStatusCode(ElementCountAnalyser):
#    __metaclass__ = AnalyserFactory
    def analyse_Dataset(self, dataset):
        self.add(dataset.status)

    def update_PortalMetaData(self, pmd):
        if not pmd.fetch_stats:
            pmd.fetch_stats = {}
        pmd.fetch_stats['respCodes'] = self.getResult()

    def analyse_PortalMetaData(self, pmd):
        if pmd.fetch_stats and 'respCodes' in pmd.fetch_stats:
            resp_codes = pmd.fetch_stats['respCodes']
            for code in resp_codes:
                self.add(code, resp_codes[code])
            return resp_codes
        return {}
        
        
class ResourceStatusCode(ElementCountAnalyser):
    def analyse_Resource(self, res):
        self.add(res.status)

    def analyse_PortalMetaData(self, pmd):
        if pmd.res_stats and 'respCodes' in pmd.res_stats:
            resp_codes = pmd.res_stats['respCodes']
            for code in resp_codes:
                self.add(code, resp_codes[code])
        return resp_codes if resp_codes else {}
    
    def update_PortalMetaData(self, pmd):
        if not pmd.res_stats:
            pmd.res_stats = {}
        pmd.res_stats['respCodes'] = self.getResult()
        
        