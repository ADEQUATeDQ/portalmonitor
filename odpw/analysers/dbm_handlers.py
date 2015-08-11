'''
Created on Aug 10, 2015

@author: jumbrich
'''
from odpw.analysers import Analyser
from odpw.db.models import Resource
from odpw.utils.dataset_converter import DCAT
from odpw.analysers.core import DistinctElementCount

import structlog
log =structlog.get_logger()

class DatasetInserter(Analyser):
    def __init__(self, dbm):
        self.dbm = dbm
    
    def analyse_Dataset(self, dataset):
        self.dbm.insertDataset(dataset)

class DatasetFetchInserter(Analyser):
    def __init__(self, dbm):
        self.dbm = dbm
    
    def analyse_Dataset(self, dataset):
        self.dbm.insertDatasetFetch(dataset)

class DatasetFetchUpdater(Analyser):
    def __init__(self, dbm):
        self.dbm = dbm
    
    def analyse_Dataset(self, dataset):
        self.dbm.updateDatasetFetch(dataset)

class CKANResourceInserter(Analyser):
    def __init__(self, dbm):
        self.dbm = dbm
    def analyse_Dataset(self, dataset):
        if dataset.data and 'resources' in dataset.data:
            for res in dataset.data['resources']:
                if 'url' in res:
                    tR =  Resource.newInstance(url=res['url'], snapshot=dataset.snapshot)
                    R = self.dbm.getResource(tR)
                    if not R:
                        tR.updateOrigin(pid=dataset.portal_id, did=dataset.id)
                        self.dbm.insertResource(tR)
                    else:
                        R.updateOrigin(pid=dataset.portal_id, did=dataset.id)
                        self.dbm.updateResource(R) 
                        
                        
class DCATDistributionCount(DistinctElementCount):
    def __init__(self,withDistinct=None):
        super(DCATDistributionCount, self).__init__(withDistinct=withDistinct)
        
    def analyse_Dataset(self, dataset):
        if dataset.dcat:
            for dcat_el in dataset.dcat:
                if str(DCAT.Distribution) in dcat_el.get('@type',[]):
                    if str(DCAT.accessURL) in dcat_el: 
                        url = dcat_el[str(DCAT.accessURL)][0]['@value']
                        self.analyse_generic(url)
                    elif str(DCAT.downloadURL) in dcat_el: 
                        url = dcat_el[str(DCAT.downloadURL)][0]['@value']
                        self.analyse_generic(url)
                    else:
                        log.info("No Resource URL", did=dataset.id, pid=dataset.portal_id)
                        self.analyse_generic('empty')
            
        
            
    def update_PortalMetaData(self, pmd):
        if not pmd.res_stats:
            pmd.res_stats = {}
        pmd.res_stats['total']= self.getResult()['count']
        if 'distinct' in self.getResult():
            pmd.res_stats['distinct']= self.getResult()['distinct']                        

class DCATDistributionInserter(Analyser):
    def __init__(self, dbm):
        self.dbm = dbm
    def analyse_Dataset(self, dataset):
        if dataset.dcat:
            for dcat_el in dataset.dcat:
                if str(DCAT.Distribution) in dcat_el.get('@type',[]):
                    url=None
                    if str(DCAT.accessURL) in dcat_el: 
                        url = dcat_el[str(DCAT.accessURL)][0]['@value']
                    elif str(DCAT.downloadURL) in dcat_el: 
                        url = dcat_el[str(DCAT.downloadURL)][0]['@value']
                    if url:
                        tR =  Resource.newInstance(url=url, snapshot=dataset.snapshot)
                        R = self.dbm.getResource(tR)
                        if not R:
                            tR.updateOrigin(pid=dataset.portal_id, did=dataset.id)
                            self.dbm.insertResource(tR)
                        else:
                            R.updateOrigin(pid=dataset.portal_id, did=dataset.id)
                            self.dbm.updateResource(R)
        