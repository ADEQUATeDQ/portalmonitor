'''
Created on Aug 18, 2015

@author: jumbrich


Existence of crucial meta data information
'''
from odpw.analysers import Analyser

from collections import Counter
from odpw.utils import dcat_access
## Provenance 

class ExistenceDCAT(Analyser):
    
    def __init__(self, accessFunct):
        super(ExistenceDCAT, self).__init__()
        self.af=accessFunct
        self.quality = None
        self.values = []
        self.total = 0.0
        
    def analyse_Dataset(self, dataset):
        value = self.af(dataset)
        t = 0.0
        c = 0.0
        for v in value:
            t += 1
            if v:
                c += 1
        self.total += 1
        res = c/t if t > 0 else 0
        self.values.append(res)
        return res

    def done(self):
        self.quality = sum(self.values)/self.total if self.total > 0 else 0

    def getResult(self):
        return self.quality
   
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

    def update_PortalMetaData(self, pmd):
        if not pmd.qa_stats:
            pmd.qa_stats = {}
        pmd.qa_stats['ExTe'] = self.quality
        cnt = Counter(self.values)
        pmd.qa_stats['ExTe_hist'] = dict(cnt)
##### 
#BASIC SPATIAL
#####  
class DatasetSpatialDCAT(SpatialDCAT):
    def __init__(self):
        super(DatasetSpatialDCAT, self).__init__(dcat_access.getSpatial)      

    def update_PortalMetaData(self, pmd):
        if not pmd.qa_stats:
            pmd.qa_stats = {}
        pmd.qa_stats['ExSp'] = self.quality
        cnt = Counter(self.values)
        pmd.qa_stats['ExSp_hist'] = dict(cnt)
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

    def update_PortalMetaData(self, pmd):
        if not pmd.qa_stats:
            pmd.qa_stats = {}
        pmd.qa_stats['ExRi'] = self.quality
        cnt = Counter(self.values)
        pmd.qa_stats['ExRi_hist'] = dict(cnt)


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


#### ANY and AVERAGE analyser
class AnyMetric(Analyser):
    def __init__(self, analyser, id):
        super(AnyMetric, self).__init__()
        self.analyser = analyser
        self.total = 0.0
        self.count = 0
        self.id = id

    def analyse_Dataset(self, dataset):
        e = any([a.analyse_Dataset(dataset) for a in self.analyser])
        self.total += 1
        if e:
            self.count += 1
        return e

    def getResult(self):
        return {'count': self.count, 'total': self.total}

    def getValue(self):
        return self.count/self.total if self.total > 0 else 0

    def name(self):
        return '_'.join([a.name() for a in self.analyser])

    def update_PortalMetaData(self, pmd):
        if not pmd.qa_stats:
            pmd.qa_stats = {}
        pmd.qa_stats[self.id] = self.getValue()
        pmd.qa_stats[self.id + '_hist'] = {1: self.count, 0: self.total - self.count}

class AverageMetric(Analyser):
    def __init__(self, analyser, id):
        super(AverageMetric, self).__init__()
        self.analyser = analyser
        self.total = 0
        self.values = []
        self.id = id

    def analyse_Dataset(self, dataset):
        count = 0.0
        t = 0.0
        for a in self.analyser:
            v = a.analyse_Dataset(dataset)
            t += 1
            count += v
        res = count/t if t > 0 else 0

        self.total += 1
        self.values.append(res)
        return res

    def getResult(self):
        return {'values': self.values, 'total': self.total}

    def getValue(self):
        return sum(self.values)/self.total if self.total > 0 else 0

    def name(self):
        return '_'.join([a.name() for a in self.analyser])

    def update_PortalMetaData(self, pmd):
        if not pmd.qa_stats:
            pmd.qa_stats = {}
        pmd.qa_stats[self.id] = self.getValue()
        cnt = Counter(self.values)
        pmd.qa_stats[self.id + '_hist'] = dict(cnt)