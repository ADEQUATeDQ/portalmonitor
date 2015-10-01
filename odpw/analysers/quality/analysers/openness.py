import os
import string

from odpw.analysers import Analyser
from odpw.analysers.quality.analysers import ODM_formats
from odpw.utils.licenses_mapping import LicensesOpennessMapping

__author__ = 'jumbrich'

import json
import numpy as np
import analyze_resource_format

LICENSES_PATH = '/usr/local/opendatawu/resources/licenses.json'

OPEN_FORMATS = ['dvi', 'svg'] + ODM_formats.get_non_proprietary()


MACHINE_FORMATS = ODM_formats.get_machine_readable()

class OpennessAnalyser(Analyser):
    
    id='Qo'
    def __init__(self):
        # license rating
        self.license_rating = LicensesOpennessMapping()

        #retrieval stats
        self.quality = {'format': 0,
                        'license': 0,
                        'total': 0
        }

        #open formats for at least one resource for a dataset
        self.openformats = []

        #open formats for at least one resource for a dataset
        self.opentotal = []

        #open licenses for at least one resource for a dataset
        self.openlicenses = []

    def analyse_Dataset(self, dataset):
        data = dataset.data
        # if no dict, return (e.g. access denied)
        if not isinstance(data, dict):
            return
        
        quality = {'format':0, 'license':0, 'total':0}

        open = self._format_openess(data)
        self.openformats.append(open)
        self.opentotal.append(open)
        
        quality['format']=open
        quality['total']+=open
        
        open = self._license_openess(data)
        self.openlicenses.append(open)
        self.opentotal.append( open)
        quality['license']=open
        quality['total']+=open
        
        
        quality['total']=quality['total']/2

        if not dataset.qa_stats:
            dataset.qa_stats={}
        dataset.qa_stats[OpennessAnalyser.id] = quality
        

    def getResult(self):
        return {OpennessAnalyser.id: self.quality}
        
    def update_PortalMetaData(self, pmd):
        if not pmd.qa_stats:
            pmd.qa_stats = {}
        pmd.qa_stats[OpennessAnalyser.id] = self.quality

    def done(self):


        self.quality['format'] = np.array(self.openformats).mean() if len(self.openformats) else None
        self.quality['license'] = np.array(self.openlicenses).mean()if len(self.openlicenses) else None
        self.quality['total'] = np.array(self.opentotal).mean()if len(self.opentotal) else None

    def _format_openess(self, data):
        open = False
        if 'resources' in data:
            for res in data['resources']:
                fv = res.get("format", str(None))
                #format = analyze_resource_format.get_format(fv).lower()
                format = fv.lower()
                if format in OPEN_FORMATS:
                    open = True
                    break
                    #else:
                    #   print format

        return 1 if open else 0
        

    def _license_openess(self, data):
        license_title = data.get('license', str(None))
        license_id = data.get('license_id', str(None))
        license_url = data.get('license_url', str(None))

        lid, openness = self.license_rating.map_license(license_title, license_id, license_url)

        return 1 if openness == 'approved' else 0
        

## NOT WORKING!!!!!!!
def _is_open_license(title, lid, url):
    with open(LICENSES_PATH) as f:
        json_data = json.load(f)

    candidates = []

    if lid:
        d = (lid.split('-'))
        for i in range(len(d), 0, -1):
            join = '-'.join(d[:i])
            for row in json_data['license']:
                if join.lower() in (row['license_id']).encode('utf-8').lower() and (row not in candidates):
                    candidates.append(row)
            if len(candidates) > 0:
                break
    else:
        for row in json_data['license']:
            candidates.append(row)

    if not url or url == '':
        pass
    else:
        b = []
        for candidate in candidates:
            for x in candidate['license_url']:
                if (url in str(x) or str(x) in url) and (candidate not in b):
                    b.append(candidate)
        if len(b) > 0:
            candidates = b

    if not title or title == '':
        pass
    else:
        c = []
        e = (title.split())
        for i in range(len(e), 0, -1):
            join = ' '.join(e[:i])
            for candidate in candidates:
                if join.lower() in candidate['license_title'] and (candidate not in c):
                    c.append(candidate)
            if len(c) > 0:
                break
        if len(c) > 0:
            candidates = c

    if not candidates:
        return False
    else:
        op = False
        for license in candidates:
            if license['od_conformance'] == False:
                return False
            elif license['od_conformance'] == True:
                op = True

            if license['osd_conformance'] == True and not op:
                op = True

        return op
