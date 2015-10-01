'''
Created on Aug 18, 2015

@author: jumbrich
'''
from odpw.analysers.quality.new.oftype_dcat import ElementCountAnalyser
from odpw.utils.dcat_access import getDistributionLicenseTriples
from odpw.utils.licenses_mapping import LicensesOpennessMapping


class LicenseOpennessDCATAnalyser(ElementCountAnalyser):
    def __init__(self):
        self.l_mapping = LicensesOpennessMapping()
        super(LicenseOpennessDCATAnalyser, self).__init__()

    def analyse_Dataset(self, dataset):
        values = getDistributionLicenseTriples(dataset)
        for id, label, url in values:
            id, appr = self.l_mapping.map_license(label, id, url)
            self.analyse_generic(appr)
        return values