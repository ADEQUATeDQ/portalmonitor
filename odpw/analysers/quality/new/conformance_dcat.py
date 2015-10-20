from odpw.analysers import Analyser
from odpw.analysers.core import DistinctElementCount
from odpw.analysers.quality.interpret_meta_field import *
from odpw.analysers.quality.new.existence_dcat import all_subclasses
from odpw.utils import dcat_access
from odpw.utils.dataset_converter import is_valid_url

__author__ = 'sebastian'

class AnyMetric(Analyser):
    def __init__(self, analyser):
        super(AnyMetric, self).__init__()
        self.analyser = analyser
        self.total = 0.0
        self.count = 0.0

    def analyse_Dataset(self, dataset):
        e = any([a.analyse_Dataset(dataset) for a in self.analyser])
        self.total += 1
        if e:
            self.count += 1
        return e

    def getResult(self):
        return {'count': self.count, 'total': self.total}

    def getValue(self):
        return self.count/self.total if self.total > 0 else 0

class AverageMetric(DistinctElementCount):
    def __init__(self, analyser):
        super(AverageMetric, self).__init__()
        self.analyser = analyser
        self.total = 0
        self.values = []

    def analyse_Dataset(self, dataset):
        self.total += 1

        count = 0.0
        t = 0.0
        for a in self.analyser:
            t += 1
            if a.analyse_Dataset(dataset):
                count += 1
        v = count/t if t > 0 else 0
        self.values.append(v)
        return v

    def getResult(self):
        return {'values': self.values, 'total': self.total}

    def getValue(self):
        return sum(self.values)/self.total if self.total > 0 else 0

class ConformanceDCAT(DistinctElementCount):

    def __init__(self, accessFunct, evaluationFunct):
        super(ConformanceDCAT, self).__init__()
        self.af=accessFunct
        self.ef=evaluationFunct

    def analyse_Dataset(self, dataset):
        value = self.af(dataset)
        eval = []
        for v in value:
            eval.append(self.ef(v))

        e = any(eval) if len(eval) > 0 else False
        if e:
            self.analyse_generic(e)
        return e


class ConformAccessUrlDCAT(ConformanceDCAT):
    def __init__(self):
        super(ConformAccessUrlDCAT, self).__init__(dcat_access.getDistributionAccessURLs, is_valid_url)

class ConformDownloadUrlDCAT(ConformanceDCAT):
    def __init__(self):
        super(ConformDownloadUrlDCAT, self).__init__(dcat_access.getDistributionDownloadURLs, is_valid_url)

