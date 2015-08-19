'''
Created on Aug 18, 2015

@author: jumbrich


Existence of crucial meta data information
'''
from odpw.analysers import Analyser
from odpw.utils.dataset_converter import DCT, DCAT

import datetime
from odpw.analysers.core import ElementCountAnalyser
## Provenance 

class ExistenceDCAT(Analyser):
    
    def analyse_Dataset(self, dataset):
        pass
    
class ProvCreationDCAT(ElementCountAnalyser):
    def analyse_Dataset(self, dataset):
        created=None
        for dcat_el in getattr(dataset,'dcat',[]):
            if str(DCAT.Dataset) in dcat_el.get('@type',[]):
                for f in dcat_el.get(str(DCT.issued),[]):
                    try:
                        created = datetime.datetime.strptime(f['@value'].split(".")[0], "%Y-%m-%dT%H:%M:%S")
                        if created is not None:
                            break
                    except Exception as e:
                        pass
        self.add(created!=None)

class ProvContactDCAT(ExistenceDCAT):
    pass
    def analyse_Dataset(self, dataset):
        created=None
        for dcat_el in getattr(dataset,'dcat',[]):
            if str(DCAT.Dataset) in dcat_el.get('@type',[]):
                for f in dcat_el.get(str(DCAT.contactPoint),[]):
                    try:
                        contact = f['@value']
                        
                        break
                    except Exception as e:
                        pass


