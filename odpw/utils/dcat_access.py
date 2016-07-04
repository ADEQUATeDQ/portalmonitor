'''
Created on Aug 26, 2015

@author: jumbrich
'''
# -*- coding: utf-8 -*-
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

def getOrganization(dataset):

    for dcat_el in getattr(dataset,'dcat',[]):
        #TODO there is also a FOAF.Ogranisation
        if str(FOAF.Organization) in dcat_el.get('@type',[]):
            for tag in dcat_el.get(str(FOAF.name),[]):
                orga=tag['@value']
                if orga is not None and len(orga)>0:
                    return orga
    for dcat_el in getattr(dataset,'dcat',[]):
        #TODO there is also a FOAF.Ogranisation
        if str(VCARD.Organization) in dcat_el.get('@type',[]):
            for tag in dcat_el.get(str(VCARD.fn),[]):
                orga=tag['@value']
                if orga is not None and len(orga)>0:
                    return orga
    return None

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
                if '@value' in f:
                    v = f.get('@value','')
                    uri=v.strip()
                elif '@id' in f:
                    v = f.get('@id','')
                    uri=v.strip()
                    #value.append(v.strip())
                #fix some typical URL problems
                uri=uri.replace("http:// \thttp:","http:")
                uri=uri.replace("http:// http://","http://")

                value.append(uri)
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

def safe_unicode1(value):
    """ return the unicode representation of obj """
    if value is None:
        return None

    if type(value) == str:
        # Ignore errors even if the string is not proper UTF-8 or has
        # broken marker bytes.
        # Python built-in function unicode() can do this.
        value = unicode(value, "utf-8", errors="ignore")
    else:
        # Assume the value object has proper __unicode__() method
        value = unicode(value)
    return value

def getDistributionLicenseTriples(dataset):
    values = accessDistribution(dataset, DCT.license)
    triples = []
    for v in values:
        id = accessById(dataset, v, DCT.identifier)
        label = accessById(dataset, v, RDFS.label)
        try:
            s_id=safe_unicode1(id)
            s_label=safe_unicode1(label)
            s_v=safe_unicode1(v)
            triples.append((s_id, s_label, s_v ))
        except Exception as e:
            print id, label, v
            raise e
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

def getDistributionFormatWithURL(dataset, url):
    return accessDistributionWithURL(dataset, url, DCT['format'])

def getDistributionMediaTypeWithURL(dataset, url):
    return accessDistributionWithURL(dataset, url, DCAT.mediaType)

def getDistributionSizeWithURL(dataset, url):
    return accessDistributionWithURL(dataset, url, DCAT.byteSize)

def getDistributionCreationDateWithURL(dataset, url):
    return accessDistributionWithURL(dataset, url, DCT.issued)

def getDistributionModificationDateWithURL(dataset, url):
    return accessDistributionWithURL(dataset, url,DCT.modified)



def accessDistributionWithURL(dataset, url, key):
    key=str(key)
    url_dict = {u'@id': url}
    for dcat_el in getattr(dataset,'dcat',[]):
        if str(DCAT.Distribution) in dcat_el.get('@type',[]):
            access = dcat_el.get(str(DCAT.accessURL), [])
            download = dcat_el.get(str(DCAT.downloadURL), [])
            if url_dict in access or url_dict in download:
                for f in dcat_el.get(key, []):
                    if '@value' in f:
                        v = f.get('@value','')
                        return v.strip()
                    elif '@id' in f:
                        v = f.get('@id','')
                        return v.strip()
    return None

def safe_unicode(value, encoding='utf-8'):
    """Converts a value to unicode, even it is already a unicode string.
        >>> from Products.CMFPlone.utils import safe_unicode
        >>> safe_unicode('spam')
        u'spam'
        >>> safe_unicode(u'spam')
        u'spam'
        >>> safe_unicode(u'spam'.encode('utf-8'))
        u'spam'
        >>> safe_unicode('\xc6\xb5')
        u'\u01b5'
        >>> safe_unicode(u'\xc6\xb5'.encode('iso-8859-1'))
        u'\u01b5'
        >>> safe_unicode('\xc6\xb5', encoding='ascii')
        u'\u01b5'
        >>> safe_unicode(1)
        1
        >>> print safe_unicode(None)
        None
    """
    if isinstance(value, unicode):
        return value
    elif isinstance(value, basestring):
        try:
            value = unicode(value, encoding)
        except (UnicodeDecodeError):
            value = value.decode('utf-8', 'replace')
    return value

def accessById(dataset, id, key):
    key = str(key)
    for dcat_el in getattr(dataset, 'dcat', []):
        if id in dcat_el.get('@id', []):
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
