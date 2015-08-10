from odpw.analysers import Analyser
import numpy as np
from odpw.analysers.core import ElementCountAnalyser
from odpw.analysers.count_analysers import TagsCount


__author__ = 'sebastian'


class SocrataTagsCount(TagsCount):
    def analyse_Dataset(self, dataset):
        if dataset.data and 'tags' in dataset.data:
            tags = dataset.data['tags']
            if isinstance(tags, list):
                for t in tags:
                    if isinstance(t, basestring):
                        self.add(t)

    def analyse_SocrataTagsCount(self, tag_analyser):
        super(SocrataTagsCount, self).analyse_TagsCount(tag_analyser)

