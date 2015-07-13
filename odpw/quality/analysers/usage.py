__author__ = 'jumbrich'

import numpy as np

class UsageAnalyser:

    def __init__(self, keys):

        self.keys = keys

        self.size=0
        self.quality= {'total':0,
                       'core':0,
                       'extra':0,
                       'res':0}

        self.usage = {'total':np.array([]),
                       'core':np.array([]),
                       'extra':np.array([]),
                       'res':np.array([])}


    def visit(self, dataset):

        data = dataset.data
        # if no dict, return (e.g. access denied)
        if not isinstance(data, dict):
            return

        # count only opened packages
        self.size += 1

        fields = {'total':0,
                'core': len(data),
                'extra': len(data['extras']) if 'extras' in data else 0,
                'res':0
            }

        reskeys = 0
        if "resources" in data:
            reskeys = len(self.keys['res'])*1.0* len(data['resources'])
            for res in data['resources']:
                fields['res'] += len(res)

        fields['total'] = fields['core'] + fields['extra'] + fields['res']


        self.usage['core'] = np.append(
            self.usage['core'],
            fields['core'] / (len(self.keys['core'])  *1.0)
        )
        if len(self.keys['extra'])!= 0:
            self.usage['extra'] = np.append(
                self.usage['extra'],
                fields['extra'] / (len(self.keys['extra'])*1.0)
            )
        if reskeys != 0:
            self.usage['res'] = np.append(
                self.usage['res'],
                fields['res'] / (reskeys) )

        self.usage['total'] = np.append(
            self.usage['total'],
            fields['total'] /
                ( len(self.keys['core']) + len(self.keys['extra'])+ reskeys)
        )


    def computeSummary(self):
        for i in ['total', 'extra', 'core', 'res']:
            if len(self.usage[i]) ==0:
                self.quality[i] = None
            else:
                self.quality[i] = self.usage[i].mean()



    def update(self, PMD):
        stats={'qa_stats':{'Qu': self.quality}}
        PMD.updateStats(stats)
        

