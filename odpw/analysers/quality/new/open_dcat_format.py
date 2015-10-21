'''
Created on Aug 18, 2015

@author: jumbrich
'''
import csv
from odpw.analysers.core import ElementCountAnalyser

from odpw.analysers.quality.analysers import ODM_formats
from odpw.analysers.quality.new.oftype_dcat import OfTypeDCAT
from odpw.utils.dcat_access import getDistributionFormats, getDistributionMediaTypes

OPEN_FORMATS = ['dvi', 'svg'] + ODM_formats.get_non_proprietary()
MACHINE_FORMATS = ODM_formats.get_machine_readable()


class FormatOpennessDCATAnalyser(OfTypeDCAT):
    def __init__(self):
        super(FormatOpennessDCATAnalyser, self).__init__(getDistributionFormats, OPEN_FORMATS)

class FormatMachineReadableDCATAnalyser(OfTypeDCAT):
    def __init__(self):
        super(FormatMachineReadableDCATAnalyser, self).__init__(getDistributionFormats, MACHINE_FORMATS)






class IANAFormatDCATAnalyser(ElementCountAnalyser):
    id = 'ConfFo'

    def __init__(self):
        super(IANAFormatDCATAnalyser, self).__init__()
        self.quality = None
        self.names = set()
        self.mimetypes = set()
        for p in ['application.csv', 'audio.csv', 'image.csv', 'message.csv', 'model.csv', 'multipart.csv', 'text.csv',
                  'video.csv']:
            with open('../paper/iana/' + p) as f:
                reader = csv.reader(f)
                # skip header
                reader.next()
                for row in reader:
                    self.names.add(row[0])
                    self.mimetypes.add(row[1])

    def analyse_Dataset(self, dataset):
        values = []
        values += getDistributionFormats(dataset)
        values += getDistributionMediaTypes(dataset)
        if len(values) == 0:
            self.analyse_generic('no values')
        for v in values:
            v = v.encode('utf-8').strip()
            v = v.lower()
            if v.startswith('.'):
                v = v[1:]
            # resource level
            if self.is_in_iana(v):
                self.analyse_generic(True)
            else:
                self.analyse_generic(False)
        return values

    def is_in_iana(self, v):
        return v in self.names or v in self.mimetypes

    def getResult(self):
        return {IANAFormatDCATAnalyser.id: self.quality}

    def done(self):
        dist = self.getDist()
        self.quality = dist[True]/(dist[True] + dist[False]) if (dist[True] + dist[False]) > 0 else 0

    def update_PortalMetaData(self, pmd):
        if not pmd.qa_stats:
            pmd.qa_stats = {}
        pmd.qa_stats[IANAFormatDCATAnalyser.id] = self.quality