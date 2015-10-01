'''
Created on Aug 18, 2015

@author: jumbrich
'''

from odpw.analysers.quality.analysers import ODM_formats
from odpw.analysers.quality.new.oftype_dcat import OfTypeDCAT
from odpw.utils.dcat_access import getDistributionFormats

OPEN_FORMATS = ['dvi', 'svg'] + ODM_formats.get_non_proprietary()
MACHINE_FORMATS = ODM_formats.get_machine_readable()


class FormatOpennessDCATAnalyser(OfTypeDCAT):
    def __init__(self):
        super(FormatOpennessDCATAnalyser, self).__init__(getDistributionFormats, OPEN_FORMATS)

class FormatMachineReadableDCATAnalyser(OfTypeDCAT):
    def __init__(self):
        super(FormatMachineReadableDCATAnalyser, self).__init__(getDistributionFormats, MACHINE_FORMATS)

