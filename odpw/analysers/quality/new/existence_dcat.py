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
   
class DescriptionDCAT(ExistenceDCAT):  
     pass

class TemporalDCAT(DescriptionDCAT):  
     pass

class SpatialDCAT(DescriptionDCAT):  
     pass
    
class DatasetTitleDCAT(DescriptionDCAT):
    def __init__(self):
        super(DatasetTitleDCAT, self).__init__(dcat_access.getTitle)
        
class DatasetDescriptionDCAT(DescriptionDCAT):
    def __init__(self):
        super(DatasetDescriptionDCAT, self).__init__(dcat_access.getDescription)
 
class DatasetPublisherDCAT(DescriptionDCAT):
    def __init__(self):
        super(DatasetPublisherDCAT, self).__init__(dcat_access.getPublisher)
    
class DatasetCreationDCAT(DescriptionDCAT):
    def __init__(self):
        super(DatasetCreationDCAT, self).__init__(dcat_access.getCreationDate)

class DatasetModificationDCAT(DescriptionDCAT):
    def __init__(self):
        super(DatasetModificationDCAT, self).__init__(dcat_access.getModificationDate)
    
class DatasetContactDCAT(DescriptionDCAT):        
    def __init__(self):
        super(DatasetContactDCAT, self).__init__(dcat_access.getContactPoint)
        
class DatasetKeywordsDCAT(DescriptionDCAT):
    def __init__(self):
        super(DatasetKeywordsDCAT, self).__init__(dcat_access.getKeywords)

class DatasetIdentifierDCAT(DescriptionDCAT):
    def __init__(self):
        super(DatasetIdentifierDCAT, self).__init__(dcat_access.getIdentifier)

class DatasetTemporalDCAT(TemporalDCAT):
    def __init__(self):
        super(DatasetTemporalDCAT, self).__init__(dcat_access.getTemporal)    

class DatasetSpatialDCAT(SpatialDCAT):
    def __init__(self):
        super(DatasetSpatialDCAT, self).__init__(dcat_access.getSpatial)      

class DatasetThemeDCAT(DescriptionDCAT):
    def __init__(self):
        super(DatasetThemeDCAT, self).__init__(dcat_access.getTheme)      
                
class DatasetAccrualPeriodicityDCAT(DescriptionDCAT):
    def __init__(self):
        super(DatasetAccrualPeriodicityDCAT, self).__init__(dcat_access.getFrequency)


    
class LocalDCAT(ExistenceDCAT):  
     pass    
    
class DatasetLanguageDCAT(LocalDCAT):
    def __init__(self):
        super(DatasetLanguageDCAT, self).__init__(dcat_access.getLanguage)


class RightDCAT(ExistenceDCAT):
    pass
    
class ProvLicenseDCAT(RightDCAT):
    def __init__(self):
        super(ProvLicenseDCAT, self).__init__(dcat_access.getDistributionLicenses)
    


def getAllProvAnalyser():
    return [cls() for cls in ExistenceDCAT.__subclasses__()]


def getAllDescriptiveAnalyser():
    return [cls() for cls in DescriptionDCAT.__subclasses__()]

def getAllTemporalAnalyser():
    return [cls() for cls in TemporalDCAT.__subclasses__()]

def getAllSpatialAnalyser():
    return [cls() for cls in SpatialDCAT.__subclasses__()]

def getAllLocalAnalyser():
    return [cls() for cls in LocalDCAT.__subclasses__()]