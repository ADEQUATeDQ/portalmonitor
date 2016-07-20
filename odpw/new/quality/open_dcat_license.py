'''
Created on Aug 18, 2015

@author: jumbrich
'''
from odpw.analysers.core import ElementCountAnalyser
from odpw.utils.dcat_access import getDistributionLicenseTriples
from odpw.utils.licenses_mapping import LicensesOpennessMapping


class LicenseOpennessDCATAnalyser(ElementCountAnalyser):
    id = 'OpLi'

    def __init__(self):
        self.l_mapping = LicensesOpennessMapping()
        super(LicenseOpennessDCATAnalyser, self).__init__()
        self.quality = None
        self.count = 0
        self.total = 0
        self.id = LicenseOpennessDCATAnalyser.id

    def analyse_Dataset(self, dataset):
        values = getDistributionLicenseTriples(dataset)
        if len(values) ==0:
            return None
        appr = ''
        for id, label, url in values:
            id, appr = self.l_mapping.map_license(label, id, url)
            self.analyse_generic(appr)
            break

        return True if 'approved' in appr else False


