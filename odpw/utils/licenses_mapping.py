import requests

__author__ = 'neumaier'


OPEN_DEFINITION = 'http://licenses.opendefinition.org/licenses/groups/all.json'

ID_MAPPING_TO_OPEN_DEF = {
    'cc-by': 'CC-BY-4.0', # map to latest cc-by
    'cc0': 'CC0-1.0',
    'cc-zero': 'CC0-1.0',
    'cc-by-nd': None,
    'cc-by-nc-nd': None,
    'cc-by-nc-sa': None,
    'cc-nc': None,
    'cc-by-sa': None,
    'cc-nd': None,
    'CC-BY': 'CC-BY-4.0', # map to latest cc-by
    'cc-by-nc': None,
    'cc-by-igo': None,
    'cc-by-2.1': 'CC-BY-4.0', # map to latest cc-by
    'CC-BY-3.0': 'CC-BY-4.0', # map to latest cc-by
    'cc-by-4.0': 'CC-BY-4.0', # map to latest cc-by
    'uk-ogl': 'OGL-UK-3.0', # map to latest
    'ukcrown': 'ukcrown',
    'ukcrown-withrights': 'ukcrown-withrights',
    'dl-de-by-1.0': None,
    'iodl1': None,
    'iodl2': None,
    'other-nc': None,
    'other-open': None,
    'other-pd': None,
    'other-closed': None,
    'other-at': None,
    'dl-de-by-2.0': None,
    'odc-odbl': 'ODbL-1.0',
    'publiek-domein': None,
    'odc-by': 'ODC-BY-1.0',
    'odc-pddl': 'ODC-PDDL-1.0',
    'gfdl': 'GFDL-1.3-no-cover-texts-no-invariant-sections',
    'gpl-2.0': 'GPL-2.0',
    'gpl-3.0': 'GPL-3.0',
    'lgpl-2.1': 'LGPL-2.1',
    'localauth-withrights': None,
    'official-work': None,
    'against-drm': 'Against-DRM',
    'dl-de-by-nc-1.0': None
}

TITLE_MAPPING_TO_OPEN_DEF = {
    'UK Open Government Licence (OGL)': 'OGL-UK-3.0', # map to latest
    'Creative Commons Attribution': 'CC-BY-4.0', # map to latest cc-by
    'Europa Legal Notice': None,
    'cc0': 'CC0-1.0',
    'Creative Commons Attribuzione': 'CC-BY-4.0', # map to latest cc-by
    'Datenlizenz Deutschland Namensnennung': None,
    'License Not Specified': None,
    'Creative Commons CCZero': 'CC0-1.0',
    'dl-de-by-1.0': None,
    'Creative Commons Attribution Share-Alike': None,
    'Open Data Commons Open Database License (ODbL)': None,
    'Open Data Commons Attribution License': 'CC-BY-4.0', # map to latest cc-by
    'GNU Free Documentation License': 'GFDL-1.3-no-cover-texts-no-invariant-sections',
    'Other (Not Open)': 'other-closed',
    'Other (Non-Commercial)': 'other-nc',
    'Other (Attribution)': 'other-at',
    'Open Data Commons Public Domain Dedication and License (PDDL)': 'ODC-PDDL-1.0',
    'Open Data Commons Public Domain Dedication and Licence (PDDL)': 'ODC-PDDL-1.0',
    'Creative Commons Non-Commercial (Any)': None,
    'Creative Commons Attribution 3.0 Australia': None,
    'Datenlizenz Deutschland Namensnennung 2.0': None,
    'iodl2': None,
    'Italian Open Data License 2.0': None,
    'Italian Open Data License v.2.0': None
}


class LicensesOpennessMapping:

    def __init__(self):
        resp = requests.get(OPEN_DEFINITION)
        if resp.status_code != requests.codes.ok:
            raise Exception("(%s) Cannot get OpenDefinition licenses.", OPEN_DEFINITION)
        self.licenses_list = resp.json()

    def map_license(self, title, lid, url):
        res_id = None
        common_id = None

        # at first check if ID is matching
        if lid:
            if lid in ID_MAPPING_TO_OPEN_DEF and ID_MAPPING_TO_OPEN_DEF[lid]:
                common_id = ID_MAPPING_TO_OPEN_DEF[lid]
            else:
                common_id = lid

            if common_id in self.licenses_list:
                res_id = common_id

        # check if title is ID or if title is matching
        if not res_id and title:
            if title in TITLE_MAPPING_TO_OPEN_DEF and TITLE_MAPPING_TO_OPEN_DEF[title]:
                common_id = TITLE_MAPPING_TO_OPEN_DEF[title]
            else:
                common_id = title

            if common_id in self.licenses_list:
                res_id = common_id
            else:
                for l in self.licenses_list:
                    if self.licenses_list[l].get('title') == common_id:
                        res_id = l
                        break

        # check if any url is matching
        if not res_id and url:
            for l in self.licenses_list:
                if self.licenses_list[l].get('url') == url:
                    res_id = l
                    break

        # assign any possible ID if not already found
        if not res_id:
            if lid:
                res_id = lid
            else:
                res_id = title if title else url

        # return a tuple (ID, od_conformance)
        if res_id in self.licenses_list:
            return res_id, self.licenses_list[res_id].get('od_conformance', 'not found')
        return res_id, 'not found'

    def is_open(self, id):
        return 'approved' == self.get_od_conformance(id)

    def get_od_conformance(self, id):
        if id in self.licenses_list:
            return self.licenses_list[id].get('od_conformance', 'not found')
        return 'not found'
