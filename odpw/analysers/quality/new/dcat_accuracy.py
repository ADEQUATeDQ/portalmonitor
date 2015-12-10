from odpw.analysers import Analyser
from odpw.utils.dcat_access import getDistributionFormats, getDistributionAccessURLs, getDistributionDownloadURLs, \
    getDistributionFormatWithURL, getDistributionMediaTypeWithURL, getDistributionSizeWithURL

__author__ = 'sebastian'



class AccuracyFormatDCATAnalyser(Analyser):

    def __init__(self, dbm, sn):
        super(AccuracyFormatDCATAnalyser, self).__init__()
        self.quality = None
        self.values = []
        self.total = 0
        self.dbm = dbm
        self.sn = sn

    def analyse_Dataset(self, dataset):
        urls = []
        urls += getDistributionAccessURLs(dataset)
        urls += getDistributionDownloadURLs(dataset)

        for url in urls:
            format = getDistributionFormatWithURL(dataset, url)
            mime = getDistributionMediaTypeWithURL(dataset, url)
            size = getDistributionSizeWithURL(dataset, url)

            res = self.dbm.getResourceByURL(url, self.sn)
            print res.mime
            print res.size

    def done(self):
        self.quality = sum(self.values)/self.total if self.total > 0 else 0
