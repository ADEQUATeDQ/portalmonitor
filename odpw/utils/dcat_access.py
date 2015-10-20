'''
Created on Aug 26, 2015

@author: jumbrich
'''
from rdflib.namespace import RDFS
from odpw.utils.dataset_converter import DCAT, DCT, VCARD, FOAF


def accessDataset(dataset, key ):
    value=[]
    key=str(key)
    for dcat_el in getattr(dataset,'dcat',[]):
        if str(DCAT.Dataset) in dcat_el.get('@type',[]):
            for f in dcat_el.get(key,[]):
                v=None
                if '@value' in f:
                    v = f['@value']
                elif '@id' in f:
                    v = f['@id']
                value.append(v.strip())
    return value

#http://www.w3.org/TR/vocab-dcat/#Class:_Dataset
def getTitle(dataset):
    return accessDataset(dataset, DCT.title)

def getDescription(dataset):
    return accessDataset(dataset, DCT.description)

def getCreationDate(dataset):
    return accessDataset(dataset, DCT.issued)

def getModificationDate(dataset):
    return accessDataset(dataset, DCT.modified)

def getLanguage(dataset):
    return accessDataset(dataset, DCT.language)

def getPublisher(dataset):
    return accessDataset(dataset, DCT.publisher)

def getFrequency(dataset):
    return accessDataset(dataset, DCT.accrualPeriodicity)

def getIdentifier(dataset):
    return accessDataset(dataset, DCT.identifier)

def getSpatial(dataset):
    return accessDataset(dataset, DCT.spatial)

def getTemporal(dataset):
    return accessDataset(dataset, DCT.temporal)

def getTheme(dataset):
    return accessDataset(dataset, DCAT.theme)


def getKeywords(dataset):
    return accessDataset(dataset, DCAT.keyword)

def getContactPoint(dataset):
    return accessDataset(dataset, DCAT.contactPoint)

def getLandingPage(dataset):
    return accessDataset(dataset, DCAT.landingPage)


#Distribution
#http://www.w3.org/TR/vocab-dcat/#class-distribution
def accessDistribution(dataset, key ):
    value=[]
    key=str(key)
    for dcat_el in getattr(dataset,'dcat',[]):
        if str(DCAT.Distribution) in dcat_el.get('@type',[]):
            for f in dcat_el.get(key,[]):
                #print key,f
                if '@value' in f:
                    v = f['@value']
                    value.append(v)
                elif '@id' in f:
                    v = f['@id']
                    value.append(v.strip())
    return value

def getDistributionTitles(dataset):
    return accessDistribution(dataset, DCT.title)

def getDistributionDescriptions(dataset):
    return accessDistribution(dataset, DCT.description)

def getDistributionCreationDates(dataset):
    return accessDistribution(dataset, DCT.issued)

def getDistributionModificationDates(dataset):
    return accessDistribution(dataset, DCT.modified)

def getDistributionLicenses(dataset):
    return accessDistribution(dataset, DCT.license)

def getDistributionLicenseTriples(dataset):
    values = accessDistribution(dataset, DCT.license)
    triples = []
    for v in values:
        id = accessById(dataset, v, DCT.identifier)
        label = accessById(dataset, v, RDFS.label)
        triples.append((id, label, str(v)))
    return triples

def getDistributionRights(dataset):
    return accessDistribution(dataset, DCT.rights)

def getDistributionAccessURLs(dataset):
    return accessDistribution(dataset, DCAT.accessURL)

def getDistributionDownloadURLs(dataset):
    return accessDistribution(dataset, DCAT.downloadURL)

def getDistributionMediaTypes(dataset):
    return accessDistribution(dataset, DCAT.mediaType)

def getDistributionFormats(dataset):
    return accessDistribution(dataset, DCT['format'])

def accessById(dataset, id, key):
    key = str(key)
    for dcat_el in getattr(dataset, 'dcat', []):
        if str(id) in dcat_el.get('@id', []):
            for f in dcat_el.get(key, []):
                v = None
                if '@value' in f:
                    v = f['@value']
                elif '@id' in f:
                    v = f['@id']
                return v.strip()


def getDistributionByteSize(dataset):
    return accessDistribution(dataset, DCAT.byteSize)

def getContactPointValues(dataset):
    points = accessDataset(dataset, DCAT.contactPoint)
    values = []
    for p in points:
        fn = accessById(dataset, p, VCARD.fn)
        if fn:
            values.append(fn)
        mail = accessById(dataset, p, VCARD.hasEmail)
        if mail:
            values.append(mail)
    return values

def getPublisherValues(dataset):
    points = accessDataset(dataset, DCT.publisher)
    values = []
    for p in points:
        for v in [FOAF.mbox, FOAF.homepage, FOAF.name]:
            fn = accessById(dataset, p, v)
            if fn:
                values.append(fn)
    return values
