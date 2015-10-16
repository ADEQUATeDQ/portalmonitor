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
from _collections import defaultdict
log =structlog.get_logger()


class PMDResourceStatsCount(Analyser):
    
    def __init__(self):
        self.aggs=defaultdict(int)

    def analyse_PortalMetaData(self, pmd):
        if pmd.res_stats:
            for k,v in pmd.res_stats.items():
                if k in ['total', 'distinct', 'urls']:
                    if not isinstance(v, list) and not isinstance(v, dict):
                        self.aggs[k]+=v
        
    def getResult(self):
        return dict(self.aggs)
        
        
class ResourceURLValidator(Analyser):
    
    def __init__(self):
        self.valid=0
        self.total=0
        
    def analyse_Resource(self, resource):
        self.total+=1
        try:
            url = urlnorm.norm(resource.url)
            self.valid+=1
        except Exception as e:
            pass
    
    def update_PortalMetaData(self, pmd):
        if pmd.res_stats is None:
            pmd.res_stats = {}
        
        pmd.res_stats['urls']= self.getResult()['validURLs']
    
    def getResult(self):
        return {'validURLs':self.valid, 'total':self.total}
    
class ResourceCount(DistinctElementCount):
    
    def __init__(self,withDistinct=None, updateAll=False):
        super(ResourceCount, self).__init__(withDistinct=withDistinct)
        self.updateAll=updateAll
        

    def analyse_PortalMetaData(self, element):
        if element.resources >= 0:
            self.count += element.resources

    def update_PortalMetaData(self, pmd):
        exists=True
        if pmd.res_stats is None:
            pmd.res_stats = {}
            exists=False
        
        if self.updateAll:
            pmd.res_stats['total']= self.getResult()['count']
        if 'distinct' in self.getResult():
            if exists and 'distinct' in pmd.res_stats and pmd.res_stats['distinct'] !=  self.getResult()['distinct']:
                print "mismatch between distinct",pmd.res_stats['distinct'],  self.getResult()['distinct']
            else:
                pmd.res_stats['distinct']= self.getResult()['distinct']
                
        if self.updateAll:
            pmd.resources = self.getResult()['count']
            
    def analyse_Resource(self, resource):
        self.analyse_generic(resource.url)
    
    def getResult(self):
        res = super(ResourceCount,self).getResult()
        
        return res

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
    def __init__(self, total_count=True):
        super(CKANFormatCount, self).__init__()
        self.total_count = total_count

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
                    if self.total_count:
                        self.add(f, formats[f])
                    else:
                        self.add(f)

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
    def __init__(self, total_count=True):
        super(TagsCount, self).__init__()
        self.total_count = total_count

    def analyse_PortalMetaData(self, pmd):
        if pmd.general_stats and 'tags' in pmd.general_stats:
            tags = pmd.general_stats['tags']
            if isinstance(tags, dict):
                for t in tags:
                    if self.total_count:
                        self.add(t, tags[t])
                    else:
                        self.add(t)

    def analyse_TagsCount(self, tag_analyser):
        tags = tag_analyser.getResult()
        if isinstance(tags, dict):
            for t in tags:
                if self.total_count:
                    self.add(t, tags[t])
                else:
                    self.add(t)

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
        for dcat_el in getattr(dataset,'dcat',[]):
            if str(DCAT.Distribution) in dcat_el.get('@type',[]):
                url =None
                
                durl = dcat_el.get(str(DCAT.downloadURL),[])
                for du in durl:
                    url = du.get('@value',None)
                    if url: 
                        break
                    url = du.get('@id',None)
                    
                if not url:
                    aurl=dcat_el.get(str(DCAT.accessURL),[])
                    for au in aurl: 
                        url = au.get('@value',None)
                        if url: 
                            break
                        url = au.get('@id',None)
               
               
                if url:
                    try:
                        url = urlnorm.norm(url.strip())
                        # props=util.head(url)
                    except Exception as e:
                        print e
                        pass
                    
                    self.analyse_generic(url.strip())
                    #print self.count, url
                else:
                    self.empty+=1
                    
      
    def getResult(self):
        res = super(DCATDistributionCount,self).getResult()
        res['empty']=self.empty
        if self.set is not None:
            res['urls']=0
            for r in self.set:
                try:
                    url = urlnorm.norm(r.strip())
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
        for dcat_el in getattr(dataset,'dcat',[]):
            if str(DCAT.Distribution) in dcat_el.get('@type',[]):
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
        for dcat_el in getattr(dataset,'dcat',[]):
            #TODO there is also a FOAF.Ogranisation
            if str(FOAF.Organization) in dcat_el.get('@type',[]):
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
        for dcat_el in getattr(dataset,'dcat',[]):
            if str(DCAT.Dataset) in dcat_el.get('@type',[]):
                for tag in dcat_el.get(str(DCAT.keyword),[]):
                    self.add(tag['@value'])
                         
    def analyse_DCATTagsCount(self, tag_analyser):
        super(DCATTagsCount, self).analyse_TagsCount(tag_analyser)


class CKANKeysCount(ElementCountAnalyser):
    def __init__(self, keys_set=None, total_count=False):
        super(CKANKeysCount, self).__init__()
        self.keys_set = keys_set
        self.total_count = total_count

    def name(self):
        if self.keys_set:
            return self.__class__.__name__ + self.keys_set
        else:
            return super(CKANKeysCount, self).name()

    def analyse_PortalMetaData(self, pmd):
        if pmd.general_stats and 'keys' in pmd.general_stats:
            keys = pmd.general_stats['keys']
            if not isinstance(keys, dict):
                return
            if self.keys_set:
                groups = [self.keys_set]
            else:
                groups = ['core', 'extra', 'res']
            for g in groups:
                key_group = keys.get(g, {})
                for k in key_group:
                    if self.total_count:
                        self.add(k, key_group[k]['count'])
                    else:
                        self.add(k)


class CKANLicenseIDCount(ElementCountAnalyser):
    def __init__(self, total_count=True):
        super(CKANLicenseIDCount, self).__init__()
        self.total_count = total_count

    def analyse_Dataset(self, dataset):
        if dataset.data and 'license_id' in dataset.data:
            lid = dataset.data.get('license_id', 'mis')
            if isinstance(lid, basestring):
                self.add(lid)

    def analyse_PortalMetaData(self, pmd):
        if pmd.general_stats and 'licenses' in pmd.general_stats and 'id_count' in pmd.general_stats['licenses']:
            ids = pmd.general_stats['licenses']['id_count']
            if isinstance(ids, dict):
                for lid in ids:
                    if self.total_count:
                        self.add(lid, ids[lid])
                    else:
                        self.add(lid)

    def update_PortalMetaData(self, pmd):
        if not pmd.general_stats:
            pmd.general_stats = {}
        if 'licenses' not in pmd.general_stats:
            pmd.general_stats['licenses'] = {}
        pmd.general_stats['licenses']['id_count'] = self.getResult()

