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

    def analyse_Dataset(self, dataset):
        values = getDistributionLicenseTriples(dataset)
        for id, label, url in values:
            id, appr = self.l_mapping.map_license(label, id, url)
            self.analyse_generic(appr)
            break
        return values

    def done(self):
        dist = self.getDist()
        exist = dist['approved'] if 'approved' in dist else 0
        self.quality = float(exist)/sum(dist.values()) if sum(dist.values()) > 0 else 0

    def getResult(self):
        return self.quality

    def update_PortalMetaData(self, pmd):
        if not pmd.qa_stats:
            pmd.qa_stats = {}
        pmd.qa_stats[LicenseOpennessDCATAnalyser.id] = self.quality