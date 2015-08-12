__author__ = 'jumbrich'

import numpy as np

class UsageAnalyser:
    id='Qu'
    def __init__(self, keys):

        self.keys = keys

        self.size=0
        self.quality= {'total':0,
                       'core':0,
                       'extra':0,
                       'res':0}

        self.usage = {'total':[],
                       'core':[],
                       'extra':[],
                       'res':[]}


    def visit(self, dataset):

        quality={ 'total':0,'core':0,
                  'extra':0,'res':0  }

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
        
        quality['core']= fields['core'] / (len(self.keys['core'])*1.0)
        quality['extra']= fields['extra'] / (len(self.keys['extra'])*1.0) if len(self.keys['extra'])!= 0 else 0
        quality['res']= fields['res'] / (reskeys) if reskeys !=0 else 0
        quality['total']= fields['total'] / ( len(self.keys['core']) + len(self.keys['extra'])+ reskeys)
        

        for key in quality:
            self.usage[key].append(quality[key])

        dataset.updateQA({'qa':{UsageAnalyser.id:quality}})

    def computeSummary(self):
        for i in ['total', 'extra', 'core', 'res']:
            if len(self.usage[i]) ==0:
                self.quality[i] = None
            else:
                self.quality[i] = np.array(self.usage[i]).mean()



    def update(self, PMD):
        stats={'qa_stats':{UsageAnalyser.id: self.quality}}
        PMD.updateStats(stats)
        

