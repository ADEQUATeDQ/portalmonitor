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
        e = any(value) if len(value) > 0 else False
        if e:
            self.analyse_generic(e)
        #print self.name(), value, all(value),e
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



class DistributionTitleDCAT(BasicDescriptionDCAT):
    def __init__(self):
        super(DistributionTitleDCAT, self).__init__(dcat_access.getDistributionTitles)

class DistributionDescriptionDCAT(BasicDescriptionDCAT):
    def __init__(self):
        super(DistributionDescriptionDCAT, self).__init__(dcat_access.getDistributionDescriptions)

class DistributionIssuedDCAT(ExistenceDCAT):
    def __init__(self):
        super(DistributionIssuedDCAT, self).__init__(dcat_access.getDistributionCreationDates)

class DistributionModifiedDCAT(ExistenceDCAT):
    def __init__(self):
        super(DistributionModifiedDCAT, self).__init__(dcat_access.getDistributionModificationDates)

class DistributionFormatsDCAT(ExistenceDCAT):
    def __init__(self):
        super(DistributionFormatsDCAT, self).__init__(dcat_access.getDistributionFormats)

class DistributionMediaTypesDCAT(ExistenceDCAT):
    def __init__(self):
        super(DistributionMediaTypesDCAT, self).__init__(dcat_access.getDistributionMediaTypes)

class DistributionByteSizeDCAT(ExistenceDCAT):
    def __init__(self):
        super(DistributionByteSizeDCAT, self).__init__(dcat_access.getDistributionByteSize)

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


class AccessDCAT(ExistenceDCAT):
    pass

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


class AccessUrlDCAT(AccessDCAT):
    def __init__(self):
        super(AccessUrlDCAT, self).__init__(dcat_access.getDistributionAccessURLs)

class DownloadUrlDCAT(AccessDCAT):
    def __init__(self):
        super(DownloadUrlDCAT, self).__init__(dcat_access.getDistributionDownloadURLs)

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
