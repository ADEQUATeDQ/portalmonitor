'''
Created on Jul 24, 2015

@author: jumbrich
'''
import hashlib


from odpw.analysers.core import ElementCountAnalyser, DistinctElementCount
from odpw.analysers import Analyser
import json
from odpw.db.models import Resource
from odpw.utils.licenses_mapping import LicensesOpennessMapping
from odpw.utils.timer import Timer
import datetime
import numpy as np
from odpw.analysers.quality import interpret_meta_field
#from odpw.analysers.factory import AnalyserFactory

class MD5DatasetAnalyser(Analyser):
#    __metaclass__ = AnalyserFactory
    
    def analyse_Dataset(self, dataset):
        if dataset.data and bool(dataset.data):
            d = json.dumps(dataset.data, sort_keys=True, ensure_ascii=True)
            data_md5 = hashlib.md5(d).hexdigest()
            dataset.md5=data_md5
  
class DatasetStatusCount(ElementCountAnalyser):
#    __metaclass__ = AnalyserFactory
    def analyse_Dataset(self, dataset):
        self.add(dataset.status)

    def update_PortalMetaData(self, pmd):
        if not pmd.fetch_stats:
            pmd.fetch_stats = {}
        pmd.fetch_stats['respCodes'] = self.getResult()
    
class DatasetCount(DistinctElementCount):
    def __init__(self):
        super(DatasetCount, self).__init__()
    
    def update_PortalMetaData(self, pmd):
        if not pmd.fetch_stats:
            pmd.fetch_stats = {}
        pmd.datasets = self.getResult()['count']
        pmd.fetch_stats['datasets'] = self.getResult()['count']

class CKANResourceInDS(DistinctElementCount):
    def __init__(self,withDistinct=None):
        super(CKANResourceInDS, self).__init__(withDistinct=withDistinct)
        
    def analyse_Dataset(self, dataset):
        if dataset.data and 'resources' in dataset.data:
            for res in dataset.data['resources']:
                if 'url' in res:
                    super(CKANResourceInDS,self).analyse(res['url'])
                else:
                    super(CKANResourceInDS,self).analyse('NA')

    def update_PortalMetaData(self,pmd):
        if not pmd.res_stats:
            pmd.res_stats = {}
        pmd.resources = self.getResult()['count']
        pmd.res_stats['total'] = self.getResult()['count']
        pmd.res_stats['unique'] = self.getResult()['distinct']

class DatasetFetchInserter(Analyser):
    def __init__(self, dbm):
        self.dbm = dbm
    
    def analyse_Dataset(self, dataset):
        self.dbm.insertDatasetFetch(dataset)

class DatasetFetchUpdater(Analyser):
    def __init__(self, dbm):
        self.dbm = dbm
    
    def analyse_Dataset(self, dataset):
        self.dbm.updateDatasetFetch(dataset)



class CKANResourceInserter(Analyser):
    def __init__(self, dbm):
        self.dbm = dbm
    def analyse_Dataset(self, dataset):
        if dataset.data and 'resources' in dataset.data:
            for res in dataset.data['resources']:
                if 'url' in res:
                    tR =  Resource.newInstance(url=res['url'], snapshot=dataset.snapshot)
                    R = self.dbm.getResource(tR)
                    if not R:
                        #R = Resource.newInstance(url=res['url'], snapshot=dataset.snapshot)
                        tR.updateOrigin(pid=dataset.portal_id, did=dataset.id)
                        self.dbm.insertResource(tR)
                    else:
                        R.updateOrigin(pid=dataset.portal_id, did=dataset.id)
                        self.dbm.updateResource(R) 




class CKANDatasetAge(Analyser):
    
    def __init__(self):
        self.ages = {'created':[],'modified':[]}
        
    def analyse_Dataset(self, dataset):
        data = dataset.data
        if data:
            if 'metadata_created' in data and data['metadata_created'] is not None:
                try:
                    created = datetime.datetime.strptime(data['metadata_created'].split(".")[0], "%Y-%m-%dT%H:%M:%S")
                    self.ages['created'].append(created)
                except Exception as e:
                    pass

            if 'metadata_modified' in data and data['metadata_modified'] is not None:
                try:
                    modified = datetime.datetime.strptime(data['metadata_modified'].split(".")[0], "%Y-%m-%dT%H:%M:%S")
                    self.ages['modified'].append(modified)
                except Exception as e:
                    pass

    def done(self):
        dsc = np.array(self.ages['created'],dtype='datetime64[us]')
        dsm = np.array(self.ages['modified'],dtype='datetime64[us]')
        if dsc.size != 0:
            delta = np.array(dsc - dsc.min())

        if dsm.size != 0:
            deltam = np.array(dsm - dsm.min())

        now = datetime.datetime.now().isoformat()
        self.age={
            'created': {
                'old': dsc.min().astype(datetime.datetime).isoformat() if dsc.size !=0 else now,
                'new': dsc.max().astype(datetime.datetime).isoformat() if dsc.size !=0 else now,
                'avg': (dsc.min()+delta.mean()).astype(datetime.datetime).isoformat() if dsc.size !=0 else now
            },
            'modified': {
                'old': dsm.min().astype(datetime.datetime).isoformat() if dsm.size !=0 else now,
                'new': dsm.max().astype(datetime.datetime).isoformat() if dsm.size !=0 else now,
                'avg': (dsm.min()+deltam.mean()).astype(datetime.datetime).isoformat() if dsm.size !=0 else now
            }
        }
    def getResult(self):
        return self.age
    
    def update_PortalMetaData(self, pmd):
        if not pmd.general_stats:
            pmd.general_stats = {}
        pmd.general_stats['dsage'] = self.getResult()
    
class CKANResourceInDSAge(CKANDatasetAge):

    def analyse_Dataset(self, dataset):
        if dataset.data and 'resources' in dataset.data:
            for resource in dataset.data['resources']:
                if 'created' in resource and resource['created'] is not None:
                    try:
                        created = datetime.datetime.strptime(resource['created'].split(".")[0], "%Y-%m-%dT%H:%M:%S")
                        self.ages['created'].append(created)
                    except Exception as e:
                        pass

                if 'last_modified' in resource and resource['last_modified'] is not None:
                    try:
                        modified = datetime.datetime.strptime(resource['last_modified'].split(".")[0], "%Y-%m-%dT%H:%M:%S")
                        self.ages['modified'].append(modified)
                    except Exception as e:
                        pass
                    
    def update_PortalMetaData(self, pmd):
        if not pmd.general_stats:
            pmd.general_stats = {}
        pmd.general_stats['resage'] = self.getResult()

class CKANKeyAnalyser(Analyser):
    
    
    def __init__(self):
        self.C='core'
        self.E='extra'
        self.R='res'
        self.DS = 'ds'
        self.RES = 'res'
        
        self.reskey= {}
        
        #appearing meta data fields
        self.keys = {
            'core': {},
            'extra': {},
            'res': {}
        }
        
        # delete fields afterwards
        self.freq= {
            'core': {},
            'extra': {},
            'res': {},
        }
        
        #actual number of datasets and resource analysed
        self.size = {
            self.DS:0,
            self.RES:0
        }
    
    def analyse_Dataset(self, element):
        data = element.data
        if not data:
            return 
        self.size[self.DS] += 1
        for field in data:
            fv = data.get(field, str(None))

            if fv is None or fv == "":
                fv = 'NA'

            if isinstance(fv, list):
                # field value is a list
                if field == 'resources':
                    self.size[self.RES] += len(fv)
                    res = {}
                    for resource in fv:
                        self.__updateResource(resource,res)

                    for k in res:
                        if k not in self.reskey:
                            self.reskey[k] = np.array([], dtype=object)

                        #list with average compl per dataset over DS resources
                        self.reskey[k] = np.append(self.reskey[k], res[k].sum()/(len(fv)*1.0))

                    fv = 'list'
                elif len(fv) == 0:
                    fv = 'NA'
                elif field == 'extras':
                    self.__updateExtras(fv)
                    fv = 'list'
                else:
                    # just add the values in the list
                    # for v in fv:
                    #    if isinstance(v, unicode):
                    #        cnt = self.freqCounts[field].get(v, 0)
                    #        self.freqCounts[field][v] = cnt + 1
                    fv = 'list'

            if isinstance(fv, dict):
                if field == 'extras':
                    # update extrafields entry
                    self.__updateExtras(fv)
                    fv = 'dict'
                elif len(fv) == 0:
                    fv = 'NA'
                else:
                    # just say that it is a dict
                    fv = 'dict'

            if field not in self.freq[self.C]:
                self.freq[self.C][field] = np.array([], dtype=object)
            a = self.freq[self.C][field]
            self.freq[self.C][field] = np.append(a, fv)
            
            
    def __updateExtras(self, extras):
        for f in extras:
            field = f
            if isinstance(extras, dict):
                fv = extras.get(field, str(None))
            else:
                fv = field
            if fv is None or fv == "":
                fv = 'NA'
            if isinstance(fv, dict):
                if len(fv) == 0:
                    fv = 'NA'
                else:
                    if 'key' in fv and 'value' in fv:
                        field = fv['key']
                        fv = fv['value']
                    else:
                        fv = 'dict'
            if isinstance(fv, list):
                if len(fv) == 0:
                    fv = 'NA'
                else:
                    fv = 'list'
            if field not in self.freq[self.E]:
                self.freq[self.E][field] = np.array([], dtype=object)

            a = self.freq[self.E][field]
            self.freq[self.E][field] = np.append(a, fv)
    
    def __updateResource(self, resource,res):
        for field in resource:
            fv = resource.get(field, str(None))
            if fv is None or fv == "":
                fv = 'NA'
            if isinstance(fv, dict):
                if len(fv) == 0:
                    fv = 'NA'
                else:
                    fv = 'dict'
            if isinstance(fv, list):
                if len(fv) == 0:
                    fv = 'NA'
                else:
                    fv = 'list'

            if field not in self.freq[self.R]:
                self.freq[self.R][field] = np.array([])
            self.freq[self.R][field] = np.append(self.freq[self.R][field], fv)

            if field not in res:
                res[field] = np.array([], dtype=object)
            a = res[field]
            if fv == 'NA':
                res[field] = np.append(a, 0)
            else:
                res[field] = np.append(a, 1)
                
    def done(self):
        #aggregate the core keys
        self.__aggregateKeys(self.freq[self.C], self.keys[self.C])

        self.__aggregateKeys(self.freq[self.E], self.keys[self.E])

        self.__aggregateResKeys(self.freq[self.R], self.keys[self.R])


    def getResult(self):
        return self.keys


    def __aggregateResKeys(self, counts, stats):
        # core fields

        a = self.reskey

        for field in a:
                # store frequency analysis of field entries
            #values.add(PortalFieldValues(self.portal_id, self.snapshot, field, counts[field]))

            stats[field] = {
                'count': len( np.where( a[field] != 0.0)[0] ),
                'mis': len( np.where(a[field] == 0.0)[0] ),
            }
            # completeness
            stats[field]['compl'] = self.__calc_completeness(stats[field])
            # availability
            stats[field]['usage'] = self.__calc_availability(stats[field], self.size[self.DS])

        for field in counts:
            distinct = np.unique(counts[field])
            types = self.__computeTypes(counts[field])

            stats[field]['res'] = {
                'count': len( np.where( counts[field] != 'NA')[0] ),
                'mis': len(np.where(counts[field] == 'NA')[0] ),
                'distinct': len(np.where( distinct != 'NA')[0] ),
                'types': types
            }
            # completeness
            stats[field]['res']['compl'] = self.__calc_completeness(stats[field]['res'])
            # availability
            stats[field]['res']['usage'] = self.__calc_availability(stats[field]['res'], self.size[self.RES])

    def __aggregateKeys(self, counts, stats):
        # core fields

        a = counts

        for field in a:
                # store frequency analysis of field entries
            #values.add(PortalFieldValues(self.portal_id, self.snapshot, field, counts[field]))

            distinct = np.unique(a[field])
            stats[field] = {
                'count': len( np.where( counts[field] != 'NA')[0] ),
                'mis': len(np.where(counts[field] == 'NA')[0] ),
                'distinct': len(np.where( distinct != 'NA')[0] )
            }


            types = self.__computeTypes(counts[field])
            stats[field]['types'] = types

            # completeness
            stats[field]['compl'] = self.__calc_completeness(stats[field])
            # availability
            stats[field]['usage'] = self.__calc_availability(stats[field], self.size[self.DS])

    def __calc_completeness(self, field_entry):
        if field_entry['count'] + field_entry['mis'] > 0:
            return float(field_entry['count']) / float(field_entry['count'] + field_entry['mis'])
        else:
            return 0

    def __calc_availability(self, field_entry, count):
        if count > 0:
            return float(field_entry['count'] + field_entry['mis']) / float(count)
        else:
            return 0
        
    def __computeTypes(self, dict):
        types = {}
        for key in dict:
            type = str(interpret_meta_field.get_type(key))

            # we don't want 'empty' as a type
            if type != 'empty':
                cnt = types.get(type, 0)
                types[type] = cnt + 1
        return types
    
    def update_PortalMetaData(self,pmd):
        if not pmd.general_stats:
            pmd.general_stats = {}
        pmd.general_stats['keys'] = self.getResult()


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
        self.conformance = {}

    def analyse_Dataset(self, dataset):
        if dataset.data:
            id = dataset.data.get('license_id')
            url = dataset.data.get('license_url')
            title = dataset.data.get('license_title')

            id, od_conformance = self.license_mapping.map_license(title=title, lid=id, url=url)
            # add id to ElementCountAnalyser
            self.add(id)

            # TODO store conformance values for IDs
            self.conformance[id] = od_conformance

    def update_PortalMetaData(self, pmd):
        if not pmd.general_stats:
            pmd.general_stats = {}
        pmd.general_stats['licenses'] = self.getResult()

    def analyse_PortalMetaData(self, pmd):
        if pmd.general_stats and 'licenses' in pmd.general_stats:
            licenses = pmd.general_stats['licenses']
            if isinstance(licenses, dict):
                for l in licenses:
                    self.add(l, licenses[l])
                    self.conformance[l] = self.license_mapping.get_od_conformance(l)

    def analyse_CKANLicenseCount(self, licenses_analyser):
        licenses, od_conf = licenses_analyser.getResult()
        if isinstance(licenses, dict):
            for l in licenses:
                self.add(l, licenses[l])
                self.conformance[l] = od_conf[l] if l in od_conf else 'not found'

    def getResult(self):
        return self.getDist(), self.conformance




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
        if pmd.general_stats and 'licenses' in pmd.general_stats:
            orgs = pmd.general_stats['licenses']
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

