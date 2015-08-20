import copy
from odpw.util import defaultdict
from odpw.analysers import Analyser

__author__ = 'sebastian'



class CKANKeyIntersectionAnalyser(Analyser):
    def __init__(self):
        self.total = defaultdict(set)
        self.intersection = defaultdict(set)
        self.sym_diff = defaultdict(set)

    def analyse_PortalMetaData(self, pmd):
        if pmd.general_stats and 'keys' in pmd.general_stats:
            keysdict = pmd.general_stats['keys']
            for group in keysdict:
                keys = keysdict[group].keys()

                self.total[group].update(keys)

                if len(self.intersection[group]) == 0:
                    self.intersection[group].update(keys)
                else:
                    self.intersection[group].intersection_update(keys)

                if len(self.sym_diff[group]) == 0:
                    self.sym_diff[group].update(keys)
                else:
                    self.sym_diff[group].symmetric_difference_update(keys)

    def getResult(self):
        return {'total': self.total, 'intersection': self.intersection, 'sym_diff': self.sym_diff}


class OGDMetadatenAnalyser(Analyser):
    def __init__(self):
        self.mandatory_usage = copy.deepcopy(OGDMetadatenAnalyser.MANDATORY)
        self.optional_usage = copy.deepcopy(OGDMetadatenAnalyser.OPTIONAL)
        self.mandatory_compl = copy.deepcopy(OGDMetadatenAnalyser.MANDATORY)
        self.optional_compl = copy.deepcopy(OGDMetadatenAnalyser.OPTIONAL)

    MANDATORY = {
        'extra': ['metadata_identifier', 'metadata_modified', 'categorization', 'publisher', 'begin_datetime'],
        'core': ['title', 'notes', 'tags', 'maintainer', 'license'],
        'res': ['url', 'format']
    }
    OPTIONAL = {
        'extra': ['schema_name', 'schema_language', 'schema_characterset', 'metadata_linkage', 'attribute_description',
                  'maintainer_link', 'geographic_toponym', 'geographic_bbox', 'end_datetime', 'update_frequency',
                  'lineage_quality', 'en_title_and_desc', 'license_citation', 'metadata_original_portal'],
        'core': ['maintainer_email'],
        'res': ['url', 'format', 'name', 'created', 'last_modified', 'size', 'language', 'characterset']
    }

    def analyse_PortalMetaData(self, pmd):
        penalty = 0.5

        if pmd.general_stats and 'keys' in pmd.general_stats:
            keysdict = pmd.general_stats['keys']
            for group in keysdict:
                keys = keysdict[group]
                for k in keys:
                    if k in self.mandatory_usage[group] and keys[k]['usage'] >= penalty:
                        self.mandatory_usage[group].remove(k)
                    if k in self.mandatory_compl[group] and keys[k]['compl'] >= penalty:
                        self.mandatory_compl[group].remove(k)

                    if k in self.optional_usage[group] and keys[k]['usage'] >= penalty:
                        self.optional_usage[group].remove(k)
                    if k in self.optional_compl[group] and keys[k]['compl'] > penalty:
                        self.optional_compl[group].remove(k)

            #count = 0.0
            #avg_compl = 0.0
            #avg_usage = 0.0
            #for k in keysdict['extra']:
                #if k in OGDMetadatenAnalyser.OPTIONAL or k in OGDMetadatenAnalyser.MANDATORY:
                #    pass
                #else:
                #    count += 1
                #    avg_compl +=keysdict['extra'][k]['compl']
                #    avg_usage +=keysdict['extra'][k]['usage']

            #print count
            #print 'avg_compl', avg_compl/count

            #print 'avg_usage', avg_usage/count

    def getResult(self):
        return {
            'usage': {'mandatory': self.mandatory_usage, 'optional': self.optional_usage},
            'compl': {'mandatory': self.mandatory_compl, 'optional': self.optional_compl}
        }