from odpw.quality.analysers import analyze_resource_format

__author__ = 'jumbrich'

import datetime
import numpy as np
from odpw.quality import interpret_meta_field


DS = 'ds'
RES = 'res'
C='core'
E='extra'
R='res'

class KeyAnalyser:
    def __init__(self):
    #no of datasets as reported by the API
        self.package_count = 0

        #actual number of datasets and resource analysed
        self.size = {
            DS:0,
            RES:0
        }

        # the computed quality measures
        self.quality = {}

        # age of datasets
        self.age = {}

        #appearing meta data fields
        self.keys = {
            'core': {},
            'extra': {},
            'res': {}
        }

        #format and license distribution
        self.formats = {}
        self.licenses = {}

        #resources per dataset
        self.resds = {
            'list':[]
        }


        # delete fields afterwards
        self.freq= {
            'core': {},
            'extra': {},
            'res': {},
        }

        self.avail= {
            'core': {},
            'extra': {},
            'res': {},
        }

        self.reskey= {}
        

    def getKeys(self):

        return self.freq


    def update(self, PMD):
        
        stats={'general_stats':{
                'key_stats': self.keys,
                'age': self.age,
                'formats': self.formats,
                'licenses': self.licenses,
                'resds': self.resds
          }}
        PMD.updateStats(stats)

    def visit(self, dataset):
        #update package count
        self.package_count += 1

        data = dataset.data
        # if no dict, return (e.g. access denied)
        if not isinstance(data, dict):
            return

        # just store the creation and modification date
        self.update_dataset_timeliness(data)

        # count only opened packages
        self.size[DS] += 1

        # iterate over fields in outermost layer
        for field in data:
            fv = data.get(field, str(None))

            if fv is None or fv == "":
                fv = 'NA'

            if isinstance(fv, list):
                # field value is a list
                if field == 'resources':
                    # update resource-level entry
                    self.size[RES] += len(fv)

                    self.resds['list'].append(len(fv))

                    res = {}
                    for resource in fv:
                        self.updateResource(resource,res)

                    for k in res:
                        if k not in self.reskey:
                            self.reskey[k] = np.array([], dtype=object)

                        #list with average compl per dataset over DS resources
                        self.reskey[k] = np.append(self.reskey[k], res[k].sum()/(len(fv)*1.0))

                    fv = 'list'
                elif len(fv) == 0:
                    fv = 'NA'
                elif field == 'extras':
                    self.updateExtras(fv)
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
                    self.updateExtras(fv)
                    fv = 'dict'
                elif len(fv) == 0:
                    fv = 'NA'
                else:
                    # just say that it is a dict
                    fv = 'dict'

            if field not in self.freq[C]:
                self.freq[C][field] = np.array([], dtype=object)
            a = self.freq[C][field]
            self.freq[C][field] = np.append(a, fv)



    def updateResource(self, resource,res):
        self.update_resource_timeliness(resource)

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

            if field not in self.freq[R]:
                self.freq[R][field] = np.array([])
            self.freq[R][field] = np.append(self.freq[R][field], fv)

            if field not in res:
                res[field] = np.array([], dtype=object)
            a = res[field]
            if fv == 'NA':
                res[field] = np.append(a, 0)
            else:
                res[field] = np.append(a, 1)

    


    
    def update_resource_timeliness(self, resource):
        if 'created' in resource and resource['created'] is not None:
            try:
                created = datetime.datetime.strptime(resource['created'].split(".")[0], "%Y-%m-%dT%H:%M:%S")
                self.resages['created'].append(created)
            except Exception as e:
                pass

        if 'last_modified' in resource and resource['last_modified'] is not None:
            try:
                modified = datetime.datetime.strptime(resource['last_modified'].split(".")[0], "%Y-%m-%dT%H:%M:%S")
                self.resages['modified'].append(modified)
            except Exception as e:
                pass

    def calc_timeliness(self):
        """
        Computes the min, mean and max for the various datetime list
        mean computation is done by taking the average of the deltas ( time -min )
            min + avg( times - min)
        """
        # dataset
        dsc = np.array(self.dsages['created'],dtype='datetime64[us]')
        dsm = np.array(self.dsages['modified'],dtype='datetime64[us]')
        if dsc.size != 0:
            delta = np.array(dsc - dsc.min())

        if dsm.size != 0:
            deltam = np.array(dsm - dsm.min())

        now = datetime.datetime.now().isoformat()
        self.age[DS]={
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

        dsc = np.array(self.resages['created'],dtype='datetime64[us]')
        dsm = np.array(self.resages['modified'],dtype='datetime64[us]')
        if dsc.size != 0:
            delta = np.array(dsc - dsc.min())

        if dsm.size != 0:
            deltam = np.array(dsm - dsm.min())


        self.age[RES]={
           'created': {
                'old': dsc.min().astype(datetime.datetime).isoformat() if dsc.size !=0 else now,
                'new': dsc.max().astype(datetime.datetime).isoformat() if dsc.size !=0 else now,
                'avg': (dsc.min()+delta.mean()).astype(datetime.datetime).isoformat()if dsc.size !=0 else now
            },
            'modified': {
                'old': dsm.min().astype(datetime.datetime).isoformat() if dsm.size !=0 else now,
                'new': dsm.max().astype(datetime.datetime).isoformat() if dsm.size !=0 else now,
                'avg': (dsm.min()+deltam.mean()).astype(datetime.datetime).isoformat() if dsm.size !=0 else now
            }
        }



    def computeSummary(self):
        # store all values of all fields
        values = set()

        # calculate mean creation date
        self.calc_timeliness()

        #aggregate the core keys
        self.aggregateKeys(self.freq[C], self.keys[C])

        self.aggregateKeys(self.freq[E], self.keys[E])

        self.aggregateResKeys(self.freq[R], self.keys[R])


        #calculate licenses
        self.licenses = {}
        if 'license' in self.freq[C]:
            license = self.freq[C]['license']
            counts = self.computeLicenses(license)
            self.licenses['license'] = counts
        if 'license_id' in self.freq[C]:
            license_id = self.freq[C]['license_id']
            counts = self.computeLicenses(license_id)
            self.licenses['license_id'] = counts
        if 'license_title' in self.freq[C]:
            license_title = self.freq[C]['license_title']
            counts = self.computeLicenses(license_title)
            self.licenses['license_title'] = counts
        if 'license_url' in self.freq[C]:
            license_url = self.freq[C]['license_url']
            counts = self.computeLicenses(license_url)
            self.licenses['license_url'] = counts

        # calculate formats
        if 'format' in self.freq[R]:
            formats = self.computeFormats(self.freq[R]['format'])
            self.formats = formats

        # average resource per DS
        ds = np.array(self.resds['list'])
        if len(ds) > 0:
            self.resds={ 'min': ds.min(), "avg":ds.mean(), "max": ds.max()}

        # delete not used fields
        del self.freq
        del self.dsages
        del self.resages
        del self.reskey

        # return the list of PortalFieldValues to store it in the db
        return values





    def __computeTypes(self, dict):
        types = {}
        for key in dict:
            type = str(interpret_meta_field.get_type(key))

            # we don't want 'empty' as a type
            if type != 'empty':
                cnt = types.get(type, 0)
                types[type] = cnt + 1
        return types

    def computeFormats(self, dict):
        formats = {}
        for key in dict:
            format = analyze_resource_format.get_format(key)
            cnt = formats.get(format, 0)
            formats[format] = cnt + 1
        return formats

    def computeLicenses(self, dict):
        licenses = []
        for l in np.unique(dict):
            licenses.append({'id': l, 'count': (dict ==l).sum() })
        return licenses

    def calc_completeness(self, field_entry):
        if field_entry['count'] + field_entry['mis'] > 0:
            return float(field_entry['count']) / float(field_entry['count'] + field_entry['mis'])
        else:
            return 0

    def __calc_availability(self, field_entry, count):
        if count > 0:
            return float(field_entry['count'] + field_entry['mis']) / float(count)
        else:
            return 0

    def updateQualityMeasures(self, qualitymeasures):
        self.quality_measures = qualitymeasures

    def aggregateKeys(self, counts, stats):
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
            stats[field]['compl'] = self.calc_completeness(stats[field])
            # availability
            stats[field]['usage'] = self.__calc_availability(stats[field], self.size[DS])

    def aggregateResKeys(self, counts, stats):
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
            stats[field]['compl'] = self.calc_completeness(stats[field])
            # availability
            stats[field]['usage'] = self.__calc_availability(stats[field], self.size[DS])

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
            stats[field]['res']['compl'] = self.calc_completeness(stats[field]['res'])
            # availability
            stats[field]['res']['usage'] = self.__calc_availability(stats[field]['res'], self.size[RES])
