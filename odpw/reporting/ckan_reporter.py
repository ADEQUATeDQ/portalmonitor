from collections import OrderedDict
import os
import numpy as np
import pandas as pd
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.core import ElementCountAnalyser, HistogramAnalyser, StatusCodeAnalyser
from odpw.analysers.count_analysers import DatasetCount, ResourceCount, CKANFormatCount, CKANTagsCount, CKANKeysCount, \
    CKANLicenseIDCount
from odpw.analysers.fetching import CKANKeyAnalyser, UsageAnalyser
from odpw.analysers.pmd_analysers import PMDDatasetCountAnalyser, PMDResourceCountAnalyser, CompletenessHistogram, \
    ContactabilityHistogram, OpennessHistogram, UsageHistogram
from odpw.analysers.statuscodes import DatasetStatusCount
from odpw.db.dbm import PostgressDBM
from odpw.db.models import Dataset, PortalMetaData
from odpw.reporting import plotting
from odpw.reporting.reporters import Report, Reporter, DataFramePlotReporter

from matplotlib.ticker import FuncFormatter
from matplotlib import pyplot as plt


__author__ = 'sebastian'

STATS = {
    'ds_total': -1,
    'res_total': -1,
    'unique_license_id': -1,
    'formats': -1,
    'tags': -1,
    'total_keys': -1,
    'extra_keys': -1,
    'res_keys': -1,
}


def to_percent(y, position):
    # Ignore the passed in position. This has the effect of scaling the default
    # tick locations.
    s = str(100 * y)
    # The percent symbol needs escaping in latex
    if plt.rcParams['text.usetex'] == True:
        return s + r'$\%$'
    else:
        return s + '%'


def createDir(dName):
    if not os.path.exists(dName):
        os.makedirs(dName)


class MultiScatterReporter(Reporter, DataFramePlotReporter):
    def __init__(self, data, labels, xlabel, ylabel, filename, colors=None):
        self.data = data
        self.labels = labels
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.filename = filename
        self.colors = colors

    def plotreport(self, dir):
        plotting.scatterplotComb(self.data, self.labels, self.xlabel, self.ylabel, dir, self.filename, colors=colors)

class MultiHistogramReporter(Reporter, DataFramePlotReporter):
    def __init__(self, data, labels, xlabel, ylabel, filename, bins=None, colors=None):
        self.data = data
        self.labels = labels
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.filename = filename
        self.bins = bins
        self.colors = colors

    def plotreport(self, dir):
        plotting.histplotComp(self.data, self.labels, self.xlabel, self.ylabel, dir, self.filename, bins=bins, colors=self.colors)

def report():

    #portals = dbm.getPortals(software='CKAN')

    pmd_analyser = AnalyserSet()
    #dataset_analyser = AnalyserSet()

    #key_analyser = dataset_analyser.add(CKANKeyAnalyser())
    #usage = dataset_analyser.add(UsageAnalyser(key_analyser))


    # 1. STATS
    ds_count = pmd_analyser.add(DatasetCount())
    res_count = pmd_analyser.add(ResourceCount())
    format_count = pmd_analyser.add(CKANFormatCount())
    tags_count = pmd_analyser.add(CKANTagsCount())

    key_count = pmd_analyser.add(CKANKeysCount(total_count=False))
    core_key_count = pmd_analyser.add(CKANKeysCount(keys_set='core', total_count=False))
    extra_key_count = pmd_analyser.add(CKANKeysCount(keys_set='extra', total_count=False))

    lid_count = pmd_analyser.add(CKANLicenseIDCount(total_count=True))

    # 2. Portal size distribution
    bins = [0,100,500,1000,10000,50000,100000,1000000]
    ds_histogram = pmd_analyser.add(PMDDatasetCountAnalyser(bins=bins))
    res_histogram = pmd_analyser.add(PMDResourceCountAnalyser(bins=bins))

    # 3. total num of values in url field (num of resources) vs unique and valid urls
    # 4. Portal Overlap: num of unique resources more then once -> datasets in same portal vs different portals
    # TODO resource analyser

    # 5. num of overlapping resources in pan european portal
    # resources, unique resources
    # TODO

    # 6. extra keys in one, resp. more than one, more than two, more than x portals
    res_key_count = pmd_analyser.add(CKANKeysCount(keys_set='res', total_count=False))


    # 7. same for tags
    # TODO

    # retrievability
    retr_distr = pmd_analyser.add(DatasetStatusCount())
    # TODO resource resp code distribution

    # usage and completeness
    compl_histogram = pmd_analyser.add(CompletenessHistogram())



if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    sn = 1531

    #np.arange(0,1,0.1)
    pmds = dbm.getPortalMetaDatasBySoftware(software='CKAN', snapshot=sn)
    pmd_iter = PortalMetaData.iter(pmds)

    pmd_analyser = AnalyserSet()
    bins=np.arange(0.0,1.1,0.1)
    compl_histogram = pmd_analyser.add(CompletenessHistogram(bins=bins))
    usage_histogram = pmd_analyser.add(UsageHistogram(bins=bins))
    cont_histogram = pmd_analyser.add(ContactabilityHistogram(bins=bins))
    open_histogram = pmd_analyser.add(OpennessHistogram(bins=bins))
    process_all(pmd_analyser, pmd_iter)

    col = ['black', 'white', 'dimgrey', 'lightgrey']
    keys_labels = {
        'total': "$\mathcal{K}$",
        'res': "$\mathcal{K^R}$",
        'core': "$\mathcal{K^C}$",
        'extra': "$\mathcal{K^E}$"
    }
    xlabel = "$Q_c$"
    ylabel = "Portals"
    filename = "qc.pdf"
    res = compl_histogram.getResult()
    data = OrderedDict([('total', res['total']), ('core', res['core']), ('extra', res['extra']), ('res', res['res'])])
    compl_rep = MultiHistogramReporter(data, labels=keys_labels, xlabel="$Q_u$", ylabel=ylabel, filename=filename, colors=col)

    res = usage_histogram.getResult()
    data = OrderedDict([('total', res['total']), ('core', res['core']), ('extra', res['extra']), ('res', res['res'])])
    usage_rep = MultiHistogramReporter(data, labels=keys_labels, xlabel=xlabel, ylabel=ylabel, filename="$Q_c$", colors=col)

    labels = {'email': "$Q_i^{e}$", 'url': "$Q_i^u$", 'total': "$Q_i^v$"}
    res = cont_histogram.getResult()
    data = OrderedDict([('total', res['total_total']), ('url', res['url_total']), ('email', res['email_total'])])
    con_rep = MultiHistogramReporter(data, labels=labels, xlabel="$Q_i$", ylabel="Portals", filename="qi.pdf", colors=col)

    labels = {'license': "$Q_o^l$", 'format': "$Q_o^f$"}
    res = open_histogram.getResult()
    data = OrderedDict([('license', res['license']), ('format', res['format'])])
    open_rep = MultiHistogramReporter(data, labels=labels, xlabel="$Q_o$", ylabel="Portals", filename="qo.pdf", colors=col)

    # x, y tuples for scatter plot
    # TODO data = OrderedDict([(g, (compl_histogram.data[g], usage_histogram.data[g])) for g in ['total', 'res', 'core', 'extra']])
    data = OrderedDict([(g, (compl_histogram.data[g], compl_histogram.data[g])) for g in ['total', 'res', 'core', 'extra']])
    colors = ['black', 'red', 'blue', 'green']
    scatter = MultiScatterReporter(data, keys_labels, "$Q_c$", "$Q_u$", "cvude.pdf", colors=colors)

    re = Report([con_rep, compl_rep, open_rep, scatter])
    re.plotreport('tmp')
