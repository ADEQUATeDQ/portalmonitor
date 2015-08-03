from odpw.analysers import Analyser
import numpy as np
from odpw.analysers.core import ElementCountAnalyser

__author__ = 'sebastian'


class SocrataKeyAnalyser(Analyser):

    def __init__(self):
        self.Main = 'main'
        self.Metadata = 'metadata'
        self.DS = 'ds'

        #appearing meta data fields
        self.keys = {
            self.Main: {},
            self.Metadata: {}
        }

        #actual number of datasets
        self.size = {
            self.DS: 0
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
                if len(fv) == 0:
                    fv = 'NA'
                else:
                    fv = 'list'

            if isinstance(fv, dict):
                if field == 'metadata':
                    # update metadata
                    for k in fv:
                        if k not in self.keys[self.Metadata]:
                            self.keys[self.Metadata][k] = 0
                        self.keys[self.Metadata][k] += 1

                    fv = 'dict'
                elif len(fv) == 0:
                    fv = 'NA'
                else:
                    # just say that it is a dict
                    fv = 'dict'

            if field not in self.keys[self.Main]:
                self.keys[self.Main][field] = 0
            self.keys[self.Main][field] += 1

    def done(self):
        pass

    def getResult(self):
        return self.keys


class SocrataTagsCount(ElementCountAnalyser):
    def analyse_Dataset(self, dataset):
        if dataset.data and 'tags' in dataset.data:
            tags = dataset.data['tags']
            if isinstance(tags, list):
                for t in tags:
                    if isinstance(t, basestring):
                        self.add(t)

    def update_PortalMetaData(self, pmd):
        if not pmd.general_stats:
            pmd.general_stats = {}
        # TODO maybe we don't want to save this??
        pmd.general_stats['tags'] = self.getResult()
