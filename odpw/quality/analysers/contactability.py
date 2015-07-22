__author__ = 'jumbrich'

import numpy as np
from odpw.quality.interpret_meta_field import is_empty
from odpw.quality import interpret_meta_field

class ContactabilityAnalyser:

    id='Qa'

    def __init__(self):
          #retrieval stats
        self.quality= {'email': { 'total':0, 'author':0, 'maintainer':0},
                       'url': { 'total':0, 'author':0, 'maintainer':0},
                       'total': { 'total':0, 'author':0, 'maintainer':0}
                       }

        self.stats = {
                'email': { 'total':np.array([]), 'author':np.array([]), 'maintainer':np.array([])},
                'url': { 'total':np.array([]), 'author':np.array([]), 'maintainer':np.array([])},
                'total': { 'total':np.array([]), 'author':np.array([]), 'maintainer':np.array([])}
                    }


    def visit(self, dataset):

        quality= {'email': { 'total':0, 'author':0, 'maintainer':0},
                       'url': { 'total':0, 'author':0, 'maintainer':0},
                       'total': { 'total':0, 'author':0, 'maintainer':0}
                       }
        
        # taking the top level metadata fields of the dataset into account
        data = dataset.data

        # if no dict, return (e.g. access denied)
        if not isinstance(data, dict):
            return

        author = ['author', 'author_email']
        a = [False, False ]
        main = ['maintainer', 'maintainer_email']
        m = [False, False ]

        #count emails and http urls in author fields
        for k in author:
            if k in data and not is_empty(data[k]):
                type = str(interpret_meta_field.get_type(data[k]))
                a[0] =  (type == 'email')
                a[1] =  ('url' in type and 'http' in type)
        #check emails and http urls in maintainer fields
        for k in main:
            if k in data and not is_empty(data[k]):
                type = str(interpret_meta_field.get_type(data[k]))
                m[0] =  (type == 'email')
                m[1] =  ('url' in type and 'http' in type)

        #count where an email appeared
        quality['email']['total']= 1 if (a[0] or m[0]) else 0
        quality['email']['author'] =  1 if a[0] else 0
        quality['email']['maintainer'] =1 if m[0] else 0
        #count where an http url appeared
        quality['url']['total'] =  1 if (a[1] or m[1]) else 0
        quality['url']['author'] = 1 if (a[1]) else 0
        quality['url']['maintainer'] =  1 if (m[1]) else 0

        quality['total']['total'] =  1 if (a[0] or m[0] or a[1] or m[1]) else 0
        quality['total']['author'] =  1 if (a[1] or a[0]) else 0
        quality['total']['maintainer'] =  1 if (m[0] or m[1]) else 0

        #print self.stats

        for key, value in quality.items():
            for key1, value1 in value.items():
                self.stats[key][key1] = np.append(self.stats[key][key1],quality[key][key1])

        dataset.updateQA({'qa':{ContactabilityAnalyser.id:quality}})

    def update(self, PMD):
        stats={'qa_stats':{ContactabilityAnalyser.id: self.quality}}
        PMD.updateStats(stats)
        

    def computeSummary(self):
        self.quality['email']['total'] =  self.stats['email']['total'].mean()
        self.quality['email']['author'] = self.stats['email']['author'].mean()
        self.quality['email']['maintainer'] = self.stats['email']['maintainer'].mean()


        self.quality['url']['total'] = self.stats['url']['total'].mean()
        self.quality['url']['author'] = self.stats['url']['author'].mean()
        self.quality['url']['maintainer'] = self.stats['url']['maintainer'].mean()

        self.quality['total']['total'] = self.stats['total']['total'].mean()
        self.quality['total']['author'] = self.stats['total']['author'].mean()
        self.quality['total']['maintainer'] = self.stats['total']['maintainer'].mean()


