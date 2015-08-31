'''
Created on Aug 26, 2015

@author: jumbrich
'''
from odpw.utils.dataset_converter import DCAT, DCT


def accessDataset(dataset, key ):
    value=[]
    key=str(key)
    for dcat_el in getattr(dataset,'dcat',[]):
        if str(DCAT.Dataset) in dcat_el.get('@type',[]):
            for f in dcat_el.get(key,[]):
                print key,f
                if '@value' in f:
                    v = f['@value']
                elif '@id' in f:
                    v = f['@id']
                value.append(v)
    return value

def getCreationDate(dataset):
    return accessDataset(dataset, DCT.issued)
def getModificationDate(dataset):
    return accessDataset(dataset, DCT.modified)
def getTitle(dataset):
    return accessDataset(dataset, DCT.title)
def getKeywords(dataset):
    return accessDataset(dataset, DCAT.keyword)
def getContactPoint(dataset):
    return []
    #return accessDataset(dataset, DCAT.contactPoint)

