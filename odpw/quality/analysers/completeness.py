__author__ = 'jumbrich'

import numpy as np
from odpw.quality.interpret_meta_field import is_empty

class CompletenessAnalyser:

    def __init__(self):
          #retrieval stats
        self.quality= {'total':0,
                       'core':0,
                       'extra':0,
                       'res':0}

        self.compl = {'total':np.array([]),
                       'core':np.array([]),
                       'extra':np.array([]),
                       'res':np.array([])}


    def visit(self, dataset):
        #print "____________________"
        # taking the top level metadata fields of the dataset into account
        #print dataset
        data = dataset.data

        # if no dict, return (e.g. access denied)
        if not isinstance(data, dict):
            return

        cnt= {
            't':0, 'tne':0,
            'c':0, 'cne':0,
            'e':0, 'ene':0,
            'r':0, 'rne':0,
            }


        #core fields
        compl = 0
        if isinstance(data, dict):
            for field in data:
                cnt['c'] += 1.0
                if not is_empty(data[field]):
                    cnt['cne'] += 1.0
            if cnt['c'] != 0.0:
                compl = cnt['cne']/cnt['c']

        #print "core:",cnt['cne'],"/",cnt['c'], " = ",cnt['cne']/cnt['c']
        self.compl['core'] = np.append(self.compl['core'], compl )

        # taking the extra fields of the dataset into account
        extra_fields_comp = 0
        if 'extras' in data:
            if isinstance(data['extras'], dict) and len(data['extras']) > 0:
                for field in data['extras']:
                    cnt['e'] += 1.0
                    if not is_empty(data['extras'][field]):
                         cnt['ene'] += 1.0
                if cnt['e']  != 0.0:
                    extra_fields_comp =  cnt['ene']/ cnt['e']

        #print "extra:",cnt['ene'],"/",cnt['e'], " = ",cnt['ene']/cnt['e']
        self.compl['extra'] = np.append(self.compl['extra'], extra_fields_comp )

        # taking all resource fields of the dataset into account
        non_empty_fields = 0.0
        fields = 0.0
        c =0
        if isinstance(data, dict):
            rescomp = 0
            if 'resources' in data:

                #store for each key if it has a value or not for each resource
                res = {}
                resources = data['resources']
                for resource in resources:
                    for key in resource:
                        if key not in res:
                            res[key] = np.array([])

                        if not is_empty(resource[key]):
                            res[key] = np.append(res[key], 1)
                        else:
                            res[key] = np.append(res[key], 0)

                for key in res:
                    #print "res:",key," = ", res[key]
                    cnt['r'] += 1.0
                    cnt['rne'] += res[key].mean()

                #print "res:",cnt['rne'],"/",cnt['r'], " = ",cnt['rne']/cnt['r']
                if cnt['r'] != 0.0:
                    rescomp = cnt['rne']/ cnt['r']
            self.compl['res'] = np.append(self.compl['res'], rescomp )

        #total
        cnt['t']= cnt['c']+cnt['e']+cnt['r']
        cnt['tne']= cnt['cne']+cnt['ene']+cnt['rne']
        if cnt['t'] != 0.0:
            self.compl['total'] = np.append(self.compl['total'], cnt['tne']/ cnt['t'] )
        else:
            self.compl['total'] = np.append(self.compl['total'], 0 )

    def update(self, PMD):
        stats={'qa_stats':{'Qc': self.quality}}
        PMD.updateStats(stats)

    def computeSummary(self):
        self.quality['total'] =  self.compl['total'].mean()
        self.quality['core'] =  self.compl['core'].mean()
        self.quality['extra'] =  self.compl['extra'].mean()
        self.quality['res'] =  self.compl['res'].mean()

