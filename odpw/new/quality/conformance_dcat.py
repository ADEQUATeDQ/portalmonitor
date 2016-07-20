from collections import Counter
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
        self.id = id

    def analyse_Dataset(self, dataset):
        conf_list = []
        exist_list = []
        for a in self.analyser:
            conform, exist = a.analyse_Dataset(dataset)
            conf_list.append(conform)
            exist_list.append(exist)


        if not any(exist_list):
            return None
        else:
            return any(conf_list)

    def name(self):
        return '_'.join([a.name() for a in self.analyser])


class AverageConformMetric(Analyser):
    def __init__(self, analyser, id):
        super(AverageConformMetric, self).__init__()
        self.analyser = analyser
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

        if not any(exist_list):
            return None
        else:
            return v

    def name(self):
        return '_'.join([a.name() for a in self.analyser])

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


class LicenseConform(Analyser):
    def __init__(self, id_based=False):
        super(LicenseConform, self).__init__()
        self.lm = licenses_mapping.LicensesOpennessMapping()
        self.id_based = id_based
        self.id = 'CoLi'

    def analyse_Dataset(self, dataset):
        value = dcat_access.getDistributionLicenseTriples(dataset)

        if len(value)==0:
            return None

        eval = []
        for id, label, url in value:
            if self.id_based:
                status = self.lm.get_od_conformance(id)
            else:
                mapped_id, status = self.lm.map_license(label, id, url)

            if status != 'not found':
                eval.append(id)

        return True if len(eval) > 0 else False
