'''
Created on Feb 10, 2016

@author: jumbrich
'''
from freshness import json_compare
from odpw.analysers.core import ElementCountAnalyser



class DatasetChangeCountAnalyser(ElementCountAnalyser):
    def __init__(self, datasets):
        super(DatasetChangeCountAnalyser, self).__init__()
        self.datasets=datasets
        
    def analyse_Dataset(self, dataset):
        if dataset.data and dataset.id in self.datasets:
            diffs = json_compare.jsondiff(self.datasets[dataset.id].data, dataset.data)
            for mode, selector, changes in diffs:
                try:
                    k= mode+'_'+"_".join([str(s) for s in selector])
                    self.add(k)
                except Exception as e:
                    print dataset.id, dataset.portal_id, e
            