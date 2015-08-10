'''
Created on Aug 10, 2015

@author: jumbrich
'''
from odpw.analysers.core import DistinctElementCount, ElementCountAnalyser
import urlnorm

from odpw.utils.dataset_converter import DCAT, FOAF, VCARD, DCT
import structlog
from odpw.analysers import Analyser
from odpw.utils.licenses_mapping import LicensesOpennessMapping
log =structlog.get_logger()


class ResourceCount(DistinctElementCount):
    
    def __init__(self):
        super(ResourceCount, self).__init__(withDistinct=True)
        
    def update_PortalMetaData(self, pmd):
        if not pmd.res_stats:
            pmd.res_stats = {}
        pmd.res_stats['status'] = self.getResult()
    
    def analyse_PortalMetaData(self, element):
        if element.resources>=0:
            self.count+= element.resources
            
    def analyse_Resource(self, resource):
        self.analyse_generic(resource.url)

class DatasetCount(DistinctElementCount):
    def __init__(self):
        super(DatasetCount, self).__init__()
    
    def analyse_PortalMetaData(self, element):
        if element.datasets>=0:
            self.count+= element.datasets
    
    def update_PortalMetaData(self, pmd):
        if not pmd.fetch_stats:
            pmd.fetch_stats = {}
        pmd.datasets = self.getResult()['count']
        pmd.fetch_stats['datasets'] = self.getResult()['count']
             
class CKANResourceInDS(DistinctElementCount):
    def __init__(self,withDistinct=None):
        super(CKANResourceInDS, self).__init__(withDistinct=withDistinct)
        
    def analyse_PortalMetaData(self, element):
        self.count+= element.resources
        if self.set is not None and element not in self.set:
            self.set.add(element.resources)

    def update_PortalMetaData(self,pmd):
        if not pmd.res_stats:
            pmd.res_stats = {}
        pmd.resources = self.getResult()['count']
        pmd.res_stats['total'] = self.getResult()['count']
        pmd.res_stats['unique'] = self.getResult()['distinct']

class CKANFormatCount(ElementCountAnalyser):
    def analyse_Dataset(self, dataset):
        if dataset.data and 'resources' in dataset.data:
            for resource in dataset.data['resources']:
                format = resource['format'] if 'format' in resource else "mis" 
                self.add(format)

    def analyse_PortalMetaData(self, pmd):
        if pmd.general_stats and 'formats' in pmd.general_stats:
            formats = pmd.general_stats['formats']
            if isinstance(formats, dict):
                for f in formats:
                    self.add(f, formats[f])

    def analyse_CKANFormatCount(self, format_analyser):
        formats = format_analyser.getResult()
        if isinstance(formats, dict):
            for f in formats:
                self.add(f, formats[f])

    def update_PortalMetaData(self, pmd):
        if not pmd.general_stats:
            pmd.general_stats = {}
        pmd.general_stats['formats'] = self.getResult()        
           
class CKANLicenseCount(ElementCountAnalyser):
    def __init__(self):
        super(CKANLicenseCount, self).__init__()
        self.license_mapping = LicensesOpennessMapping()
        
    def analyse_Dataset(self, dataset):
        if dataset.data:
            id = dataset.data.get('license_id')
            url = dataset.data.get('license_url')
            title = dataset.data.get('license_title')

            id, od_conformance = self.license_mapping.map_license(title=title, lid=id, url=url)
            # add id to ElementCountAnalyser
            self.add(id)

    def update_PortalMetaData(self, pmd):
        if not pmd.general_stats:
            pmd.general_stats = {}
        if 'licenses' not in pmd.general_stats or isinstance(pmd.general_stats['licenses'], list):
            pmd.general_stats['licenses']={}
        
        
            
        pmd.general_stats['licenses']['count'] = self.getResult()

    def analyse_PortalMetaData(self, pmd):
        if pmd.general_stats and 'licenses' in pmd.general_stats and 'count' in pmd.general_stats['licenses']:
            licenses = pmd.general_stats['licenses']['count']
            if isinstance(licenses, dict):
                for l in licenses:
                    self.add(l, licenses[l])
                    
    def analyse_CKANLicenseCount(self, licenses_analyser):
        licenses = licenses_analyser.getResult()
        if isinstance(licenses, dict):
            for l in licenses:
                self.add(l, licenses[l])
                
    def getResult(self):
        return self.getDist()

class CKANOrganizationsCount(ElementCountAnalyser):
    def analyse_Dataset(self, dataset):
        if dataset.data and 'organization' in dataset.data:
            org = dataset.data['organization']
            if isinstance(org, dict):
                if 'name' in org:
                    self.add(org['name'])

    def update_PortalMetaData(self, pmd):
        if not pmd.general_stats:
            pmd.general_stats = {}
        pmd.general_stats['organizations'] = self.getResult()

    def analyse_PortalMetaData(self, pmd):
        if pmd.general_stats and 'organizations' in pmd.general_stats:
            orgs = pmd.general_stats['organizations']
            if isinstance(orgs, dict):
                for o in orgs:
                    self.add(o, orgs[o])

    def analyse_CKANOrganizationsCount(self, org_analyser):
        orgs = org_analyser.getResult()
        if isinstance(orgs, dict):
            for o in orgs:
                self.add(o, orgs[o])

class TagsCount(ElementCountAnalyser):
    def analyse_PortalMetaData(self, pmd):
        if pmd.general_stats and 'tags' in pmd.general_stats:
            tags = pmd.general_stats['tags']
            if isinstance(tags, dict):
                for t in tags:
                    self.add(t, tags[t])

    def analyse_TagsCount(self, tag_analyser):
        tags = tag_analyser.getResult()
        if isinstance(tags, dict):
            for t in tags:
                self.add(t, tags[t])

    def update_PortalMetaData(self, pmd):
        if not pmd.general_stats:
            pmd.general_stats = {}
        pmd.general_stats['tags'] = self.getResult()

class CKANTagsCount(TagsCount):
    def analyse_Dataset(self, dataset):
        if dataset.data and 'tags' in dataset.data:
            tags = dataset.data['tags']
            if isinstance(tags, list):
                for t in tags:
                    if isinstance(t, dict):
                        if 'name' in t:
                            self.add(t['name'])
                    elif isinstance(t, basestring):
                        self.add(t)

    def analyse_CKANTagsCount(self, tag_analyser):
        super(CKANTagsCount, self).analyse_TagsCount(tag_analyser)
      
class DCATDistributionCount(DistinctElementCount):
    def __init__(self,withDistinct=None):
        super(DCATDistributionCount, self).__init__(withDistinct=withDistinct)
        self.empty=0
    def analyse_Dataset(self, dataset):
        if dataset.dcat:
            for dcat_el in dataset.dcat:
                if str(DCAT.Distribution) in dcat_el['@type']:
                    if str(DCAT.accessURL) in dcat_el: 
                        url = dcat_el[str(DCAT.accessURL)][0]['@value']
                        self.analyse_generic(url)
                    elif str(DCAT.downloadURL) in dcat_el: 
                        url = dcat_el[str(DCAT.downloadURL)][0]['@value']
                        self.analyse_generic(url)
                    else:
                        log.info("No Resource URL", did=dataset.id, pid=dataset.portal_id)
                        self.empty+=1
            
        else:
            print "no dcat"
      
    def getResult(self):
        res = super(DCATDistributionCount,self).getResult()
        res['empty']=self.empty
        if self.set is not None:
            res['urls']=0
            for r in self.set:
                try:
                    url = urlnorm.norm(r)
                    res['urls']+=1
                except Exception as e:
                    pass
        return res
        
    def update_PortalMetaData(self, pmd):
        if not pmd.res_stats:
            pmd.res_stats = {}
        pmd.res_stats['total']= self.getResult()['count']
        if 'distinct' in self.getResult():
            pmd.res_stats['distinct']= self.getResult()['distinct']
            pmd.res_stats['urls']= self.getResult()['urls']
        pmd.resources = self.getResult()['count']




class DCATFormatCount(ElementCountAnalyser):
    def analyse_Dataset(self, dataset):
        if dataset.dcat:
            for dcat_el in dataset.dcat:
                if str(DCAT.Distribution) in dcat_el['@type']:
                    for f in dcat_el.get('http://purl.org/dc/terms/format',[]):
                        self.add(f['@value'])
        
    def analyse_PortalMetaData(self, pmd):
        if pmd.general_stats and 'formats' in pmd.general_stats:
            formats = pmd.general_stats['formats']
            if isinstance(formats, dict):
                for f in formats:
                    self.add(f, formats[f])

    def analyse_CKANFormatCount(self, format_analyser):
        formats = format_analyser.getResult()
        if isinstance(formats, dict):
            for f in formats:
                self.add(f, formats[f])

    def update_PortalMetaData(self, pmd):
        if not pmd.general_stats:
            pmd.general_stats = {}
        pmd.general_stats['formats'] = self.getResult()
        
class DCATLicenseCount(ElementCountAnalyser):
    def __init__(self):
        super(DCATLicenseCount, self).__init__()
        self.license_mapping = LicensesOpennessMapping()
        
    def analyse_Dataset(self, dataset):
        pass
    

    def getResult(self):
        return self.getDist()

class DCATOrganizationsCount(ElementCountAnalyser):
    def analyse_Dataset(self, dataset):
        if dataset.dcat:
            for dcat_el in dataset.dcat:
                #TODO there is also a FOAF.Ogranisation
                if str(FOAF.Organization) in dcat_el['@type']:
                    for tag in dcat_el.get(str(FOAF.name),[]):
                        self.add(tag['@value'])

    def update_PortalMetaData(self, pmd):
        if not pmd.general_stats:
            pmd.general_stats = {}
        pmd.general_stats['organizations'] = self.getResult()

    def analyse_PortalMetaData(self, pmd):
        if pmd.general_stats and 'organizations' in pmd.general_stats:
            orgs = pmd.general_stats['organizations']
            if isinstance(orgs, dict):
                for o in orgs:
                    self.add(o, orgs[o])

    def analyse_CKANOrganizationsCount(self, org_analyser):
        orgs = org_analyser.getResult()
        if isinstance(orgs, dict):
            for o in orgs:
                self.add(o, orgs[o])

class DCATTagsCount(TagsCount):
    def analyse_Dataset(self, dataset):
        if dataset.dcat:
            for dcat_el in dataset.dcat:
                if str(DCAT.Dataset) in dcat_el['@type']:
                    for tag in dcat_el.get(str(DCAT.keyword),[]):
                        self.add(tag['@value'])
                             
    def analyse_DCATTagsCount(self, tag_analyser):
        super(DCATTagsCount, self).analyse_TagsCount(tag_analyser)
