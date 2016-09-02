'''
Created on Aug 18, 2015

@author: jumbrich
'''
from collections import Counter
import csv
import json

from odpw.quality import Analyser,ElementCountAnalyser

from odpw.quality import ODM_formats
from odpw.core.dcat_access import getDistributionFormats, getDistributionMediaTypes

OPEN_FORMATS = ['dvi', 'svg'] + ODM_formats.get_non_proprietary()
MACHINE_FORMATS = ODM_formats.get_machine_readable()



class ContainsFormatDCATAnalyser(Analyser):

    def __init__(self, contains_set):
        super(ContainsFormatDCATAnalyser, self).__init__()
        self.quality = None
        self.values = []
        self.total = 0
        self.contains_set = contains_set

    def analyse_Dataset(self, dataset):
        formats = getDistributionFormats(dataset)
        # resource level
        t = 0.0
        c = 0.0

        if len(formats) == 0:
            return None

        for v in formats:
            t += 1
            v = v.encode('utf-8').strip()
            v = v.lower()
            if v.startswith('.'):
                v = v[1:]

            if v in self.contains_set:
                c += 1

        v = c/t if t > 0 else 0

        return v

class FormatOpennessDCATAnalyser(ContainsFormatDCATAnalyser):

    def __init__(self):
        super(FormatOpennessDCATAnalyser, self).__init__(OPEN_FORMATS)
        self.id = 'OpFo'



class FormatMachineReadableDCATAnalyser(ContainsFormatDCATAnalyser):
    def __init__(self):
        super(FormatMachineReadableDCATAnalyser, self).__init__(MACHINE_FORMATS)
        self.id = 'OpMa'





class IANAFormatDCATAnalyser(Analyser):
    def __init__(self):
        super(IANAFormatDCATAnalyser, self).__init__()
        self.id = 'CoFo'

        self.quality = None
        self.values = []
        self.total = 0
        self.names = set()
        self.endings = set()
        self.mimetypes = set()
        from pkg_resources import  resource_filename
        for p in ['application.csv', 'audio.csv', 'image.csv', 'message.csv', 'model.csv', 'multipart.csv', 'text.csv',
                  'video.csv']:
            with open(resource_filename('odpw.resources.iana', p)) as f:
                reader = csv.reader(f)
                # skip header
                reader.next()
                for row in reader:
                    self.names.add(row[0].strip().lower())
                    self.mimetypes.add(row[1].strip().lower())
        with open(resource_filename('odpw.resources.iana','mimetypes.json')) as f:
            mappings = json.load(f)
            for dict in mappings:
                for k in dict:
                    if dict[k] in self.mimetypes:
                        self.endings.add(k[1:])

    def analyse_Dataset(self, dataset):
        formats = []
        formats += getDistributionFormats(dataset)
        formats += getDistributionMediaTypes(dataset)
        if len(formats) == 0:
            return None

        # resource level
        t = 0.0
        c = 0.0
        for v in formats:
            t += 1
            v = v.encode('utf-8').strip()
            v = v.lower()
            if v.startswith('.'):
                v = v[1:]

            if self.is_in_iana(v):
                c += 1

        v = c/t if t > 0 else 0
        return v

    def analyse_IANAFormatDCATAnalyser(self, analyser):
        self.values=self.values+analyser.values
        self.total+=analyser.total


    def is_in_iana(self, v):
        return v in self.names or v in self.mimetypes or v in self.endings

