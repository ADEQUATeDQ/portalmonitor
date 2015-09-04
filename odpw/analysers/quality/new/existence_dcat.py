'''
Created on Aug 18, 2015

@author: jumbrich


Existence of crucial meta data information
'''
from odpw.analysers import Analyser
from odpw.utils.dataset_converter import DCT, DCAT

import datetime
from odpw.analysers.core import ElementCountAnalyser, DistinctElementCount
from odpw.utils import dcat_access
## Provenance 




class ExistenceDCAT(DistinctElementCount):
    
    def __init__(self, accessFunct):
        super(ExistenceDCAT, self).__init__()
        self.af=accessFunct
        
    def analyse_Dataset(self, dataset):
        value = self.af(dataset)
        e= all(value) if len(value)>0 else False
        self.analyse_generic(e)
        print self.name(), value, all(value),e
        return e
    
class ProvCreationDCAT(ExistenceDCAT):
    def __init__(self):
        super(ProvCreationDCAT, self).__init__(dcat_access.getCreationDate)

class ProvModificationDCAT(ExistenceDCAT):
    def __init__(self):
        super(ProvModificationDCAT, self).__init__(dcat_access.getModificationDate)
    
class ProvContactDCAT(ExistenceDCAT):        
    def __init__(self):
        super(ProvContactDCAT, self).__init__(dcat_access.getContactPoint)
        
class ProvKeywordsDCAT(ExistenceDCAT):
    def __init__(self):
        super(ProvKeywordsDCAT, self).__init__(dcat_access.getKeywords)
    

class ProvTitleDCAT(ExistenceDCAT):
    def __init__(self):
        super(ProvTitleDCAT, self).__init__(dcat_access.getTitle)
    


def getAllProvAnalyser():
    return [cls() for cls in ExistenceDCAT.__subclasses__()]
