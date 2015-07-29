'''
Created on Jul 24, 2015

@author: jumbrich
'''
import hashlib


from odpw.analysers.core import CountAnalyser, StatusCodeAnalyser, ElementCount
from odpw.analysers import Analyser
import json
from odpw.db.models import Resource
from odpw.utils.timer import Timer
import datetime
import numpy as np
from odpw.analysers.quality import interpret_meta_field

class MD5DatasetAnalyser(Analyser):
    
    def analyse_Dataset(self, dataset):
        if dataset.data and bool(dataset.data):
            d = json.dumps(dataset.data, sort_keys=True, ensure_ascii=True)
            data_md5 = hashlib.md5(d).hexdigest()
            dataset.md5=data_md5
  
class DatasetStatusCount(CountAnalyser):
    
    def analyse_Dataset(self, dataset):
        self.add(dataset.status)

    def update_PortalMetaData(self, pmd):
        if not pmd.fetch_stats:
            pmd.fetch_stats = {}
        pmd.fetch_stats['respCodes'] = self.getResult()
    
class DatasetCount(ElementCount):
    def __init__(self):
        super(DatasetCount, self).__init__()
    
    def update_PortalMetaData(self, pmd):
        if not pmd.fetch_stats:
            pmd.fetch_stats = {}
        pmd.datasets = self.getResult()['count']
        pmd.fetch_stats['datasets'] = self.getResult()['count']
        
    def update_Portal(self, portal):
        portal.dataset=self.getResult()['count'] 
    

class CKANResourceInDS(ElementCount):
    def __init__(self,withDistinct=None):
        super(CKANResourceInDS, self).__init__(withDistinct=withDistinct)
        
    def analyse_Dataset(self, dataset):
        if dataset.data and 'resources' in dataset.data:
            for res in dataset.data['resources']:
                super(CKANResourceInDS,self).analyse(res['url'])

    def update_PortalMetaData(self,pmd):
        if not pmd.res_stats:
            pmd.res_stats = {}
        pmd.resources = self.getResult()['count']
        pmd.res_stats['total'] = self.getResult()['count']
        pmd.res_stats['unique'] = self.getResult()['distinct']

    def update_Portal(self, portal):
        portal.resources=self.getResult()['count'] 
    

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
                    tR =  Resource.newInstance(url=res['url'], snapshot=dataset.snapshot)
                    R = self.dbm.getResource(tR)
                    if not R:
                        #do the lookup
                        with Timer(key="newRes") as t:
                            R = Resource.newInstance(url=res['url'], snapshot=dataset.snapshot)
                            self.dbm.insertResource(R)
                        
                    R.updateOrigin(pid=dataset.portal, did=dataset.dataset)
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
    
class CKANFormatCount(CountAnalyser):
    
    def analyse_Dataset(self, dataset):
        if dataset.data and 'resources' in dataset.data:
            for resource in dataset.data['resources']:
                format = resource['format'] if 'format' in resource else "mis" 
                self.add(format)

    def update_PortalMetaData(self, pmd):
        if not pmd.general_stats:
            pmd.general_stats = {}
        pmd.general_stats['formats'] = self.getResult()
