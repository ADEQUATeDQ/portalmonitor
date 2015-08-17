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

class DatasetUpdater(Analyser):
    def __init__(self, dbm):
        self.dbm = dbm
    
    def analyse_Dataset(self, dataset):
        self.dbm.updateDataset(dataset)


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
                        

class DCATDistributionInserter(Analyser):
    def __init__(self, dbm):
        self.dbm = dbm
    
    def analyse_Dataset(self, dataset):
        for dcat_el in getattr(dataset,'dcat',[]):
            if str(DCAT.Distribution) in dcat_el.get('@type',[]):
                url=None
                
                durl = dcat_el.get(str(DCAT.downloadURL),[])
                for du in durl:
                    url = du.get('@value',None)
                    if url: 
                        break
                    url = du.get('@id',None)
                    
                if not url:
                    aurl=dcat_el.get(str(DCAT.accessURL),[])
                    for au in aurl: 
                        url = au.get('@value',None)
                        if url: 
                            break
                        url = au.get('@id',None)
                if url:
                    tR =  Resource.newInstance(url=url, snapshot=dataset.snapshot)
                    R = self.dbm.getResource(tR)
                    if not R:
                        tR.updateOrigin(pid=dataset.portal_id, did=dataset.id)
                        self.dbm.insertResource(tR)
                    else:
                        R.updateOrigin(pid=dataset.portal_id, did=dataset.id)
                        self.dbm.updateResource(R)
        