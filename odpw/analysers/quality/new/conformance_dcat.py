from odpw.analysers import Analyser
from odpw.analysers.core import DistinctElementCount
from odpw.utils import dcat_access, licenses_mapping
from odpw.utils.data_utils import is_email, is_date
from odpw.utils.dataset_converter import is_valid_url

__author__ = 'sebastian'

class AnyConformMetric(Analyser):
    def __init__(self, analyser, id):
        super(AnyConformMetric, self).__init__()
        self.analyser = analyser
        self.total = 0.0
        self.count = 0.0
        self.id = id

    def analyse_Dataset(self, dataset):
        conf_list = []
        exist_list = []
        for a in self.analyser:
            conform, exist = a.analyse_Dataset(dataset)
            conf_list.append(conform)
            exist_list.append(exist)

        if any(exist_list):
            self.total += 1
        if any(conf_list):
            self.count += 1
        return any(conf_list)

    def getResult(self):
        return {'count': self.count, 'total': self.total}

    def getValue(self):
        return self.count/self.total if self.total > 0 else 0

    def name(self):
        return '_'.join([a.name() for a in self.analyser])

    def update_PortalMetaData(self, pmd):
        if not pmd.qa_stats:
            pmd.qa_stats = {}
        pmd.qa_stats[self.id] = self.getValue()

class AverageConformMetric(Analyser):
    def __init__(self, analyser, id):
        super(AverageConformMetric, self).__init__()
        self.analyser = analyser
        self.total = 0
        self.values = []
        self.id = id

    def analyse_Dataset(self, dataset):
        count = 0.0
        t = 0.0
        exist_list = []
        for a in self.analyser:
            conform, exist = a.analyse_Dataset(dataset)
            exist_list.append(exist)
            if exist:
                t += 1
            if conform:
                count += 1
        v = count/t if t > 0 else 0

        if any(exist_list):
            self.total += 1
            self.values.append(v)

        return v

    def getResult(self):
        return {'values': self.values, 'total': self.total}

    def getValue(self):
        return sum(self.values)/self.total if self.total > 0 else 0

    def name(self):
        return '_'.join([a.name() for a in self.analyser])

    def update_PortalMetaData(self, pmd):
        if not pmd.qa_stats:
            pmd.qa_stats = {}
        pmd.qa_stats[self.id] = self.getValue()

class ConformanceDCAT(DistinctElementCount):

    def __init__(self, accessFunct, evaluationFunct):
        super(ConformanceDCAT, self).__init__()
        self.af=accessFunct
        self.ef=evaluationFunct

    def analyse_Dataset(self, dataset):
        value = self.af(dataset)
        exist = False
        if len(value) > 0:
            exist = True
        eval = []
        for v in value:
            eval.append(self.ef(v))

        e = any(eval) if len(eval) > 0 else False
        if e:
            self.analyse_generic(e)
        return e, exist


class ConformAccessUrlDCAT(ConformanceDCAT):
    def __init__(self):
        super(ConformAccessUrlDCAT, self).__init__(dcat_access.getDistributionAccessURLs, is_valid_url)

class ConformDownloadUrlDCAT(ConformanceDCAT):
    def __init__(self):
        super(ConformDownloadUrlDCAT, self).__init__(dcat_access.getDistributionDownloadURLs, is_valid_url)


class EmailConformContactPoint(ConformanceDCAT):
    def __init__(self):
        super(EmailConformContactPoint, self).__init__(dcat_access.getContactPointValues, is_email)

class EmailConformPublisher(ConformanceDCAT):
    def __init__(self):
        super(EmailConformPublisher, self).__init__(dcat_access.getPublisherValues, is_email)

class UrlConformContactPoint(ConformanceDCAT):
    def __init__(self):
        super(UrlConformContactPoint, self).__init__(dcat_access.getContactPointValues, is_valid_url)

class UrlConformPublisher(ConformanceDCAT):
    def __init__(self):
        super(UrlConformPublisher, self).__init__(dcat_access.getPublisherValues, is_valid_url)

class DateConform(ConformanceDCAT):
    def __init__(self, access_function):
        super(DateConform, self).__init__(access_function, is_date)


class LicenseConform(DistinctElementCount):
    def __init__(self, id_based=False):
        super(LicenseConform, self).__init__()
        self.lm = licenses_mapping.LicensesOpennessMapping()
        self.id_based = id_based
        self.total = 0.0

    def analyse_Dataset(self, dataset):
        value = dcat_access.getDistributionLicenseTriples(dataset)
        exist = False
        if len(value) > 0:
            exist = True
            self.total += 1
        eval = []
        for id, label, url in value:
            if self.id_based:
                status = self.lm.get_od_conformance(id)
            else:
                mapped_id, status = self.lm.map_license(id, label, url)

            if status != 'not found':
                eval.append(id)

        e = True if len(eval) > 0 else False
        if e:
            self.analyse_generic(e)
        return e, exist

    def getValue(self):
        return self.count/self.total if self.total > 0 else 0

    def update_PortalMetaData(self, pmd):
        if not pmd.qa_stats:
            pmd.qa_stats = {}
        pmd.qa_stats['CoLi'] = self.getValue()