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
 
class BasicDescriptionDCAT(DescriptionDCAT):  
     pass

class TemporalDCAT(DescriptionDCAT):  
     pass

class SpatialDCAT(DescriptionDCAT):  
     pass
class LocalDCAT(DescriptionDCAT):  
     pass    
class RightDCAT(DescriptionDCAT):
    pass
 
##### 
#BASIC DESCRIPTIVE
#####     
class DatasetTitleDCAT(BasicDescriptionDCAT):
    def __init__(self):
        super(DatasetTitleDCAT, self).__init__(dcat_access.getTitle)
        
class DatasetDescriptionDCAT(BasicDescriptionDCAT):
    def __init__(self):
        super(DatasetDescriptionDCAT, self).__init__(dcat_access.getDescription)
 
class DatasetPublisherDCAT(BasicDescriptionDCAT):
    def __init__(self):
        super(DatasetPublisherDCAT, self).__init__(dcat_access.getPublisher)

class DatasetKeywordsDCAT(BasicDescriptionDCAT):
    def __init__(self):
        super(DatasetKeywordsDCAT, self).__init__(dcat_access.getKeywords)

class DatasetIdentifierDCAT(BasicDescriptionDCAT):
    def __init__(self):
        super(DatasetIdentifierDCAT, self).__init__(dcat_access.getIdentifier)

class DatasetThemeDCAT(BasicDescriptionDCAT):
    def __init__(self):
        super(DatasetThemeDCAT, self).__init__(dcat_access.getTheme)      

##### 
#BASIC TEMPORAL
#####  
class DatasetTemporalDCAT(TemporalDCAT):
    def __init__(self):
        super(DatasetTemporalDCAT, self).__init__(dcat_access.getTemporal)    

##### 
#BASIC SPATIAL
#####  
class DatasetSpatialDCAT(SpatialDCAT):
    def __init__(self):
        super(DatasetSpatialDCAT, self).__init__(dcat_access.getSpatial)      

##### 
#BASIC LOCAL
#####    

class DatasetLanguageDCAT(LocalDCAT):
    def __init__(self):
        super(DatasetLanguageDCAT, self).__init__(dcat_access.getLanguage)

    

class AdministriativeDCAT(ExistenceDCAT):
    pass

class PreservationDCAT(AdministriativeDCAT):
    pass

class PreservationTemporalDCAT(PreservationDCAT):
    pass


##### 
#BASIC RIGHT
#####  
class ProvLicenseDCAT(RightDCAT):
    def __init__(self):
        super(ProvLicenseDCAT, self).__init__(dcat_access.getDistributionLicenses)



class DatasetCreationDCAT(PreservationTemporalDCAT):
    def __init__(self):
        super(DatasetCreationDCAT, self).__init__(dcat_access.getCreationDate)

class DatasetModificationDCAT(PreservationTemporalDCAT):
    def __init__(self):
        super(DatasetModificationDCAT, self).__init__(dcat_access.getModificationDate)
    
class DatasetContactDCAT(PreservationDCAT):        
    def __init__(self):
        super(DatasetContactDCAT, self).__init__(dcat_access.getContactPoint)
        
class DatasetAccrualPeriodicityDCAT(PreservationTemporalDCAT):
    def __init__(self):
        super(DatasetAccrualPeriodicityDCAT, self).__init__(dcat_access.getFrequency)


def all_subclasses(cls):
    subs=[]
    clss= cls.__subclasses__()
    if len(clss)>0:
        for c in clss:
            subs+=all_subclasses(c)
    else:
        subs.append(cls)
        
    return subs
    
        
        

def getAllProvAnalyser():
    return [cls() for cls in all_subclasses(ExistenceDCAT)]


def getAllDescriptiveAnalyser():
    return [cls() for cls in DescriptionDCAT.__subclasses__()]

def getAllTemporalAnalyser():
    return [cls() for cls in TemporalDCAT.__subclasses__()]

def getAllSpatialAnalyser():
    return [cls() for cls in SpatialDCAT.__subclasses__()]

def getAllLocalAnalyser():
    return [cls() for cls in LocalDCAT.__subclasses__()]
