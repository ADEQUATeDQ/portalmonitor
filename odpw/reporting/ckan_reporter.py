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
from odpw.analysers.resource_analysers import ResourceOverlapAnalyser, ResourceOccurrenceCountAnalyser
from odpw.analysers.statuscodes import DatasetStatusCount
from odpw.db.dbm import PostgressDBM
from odpw.db.models import Dataset, PortalMetaData, Resource
from odpw.reporting import plotting
from odpw.reporting.reporters import Report, Reporter, DataFramePlotReporter, FormatCountReporter, TagReporter, \
    ElementCountReporter, CSVReporter, ResourceOverlapReporter

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

if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    sn = 1533

    res = dbm.getResources(snapshot=sn, portalID='data_gov')
    res_analyser = AnalyserSet()

    a1 = res_analyser.add(ResourceOverlapAnalyser())

    a2 = res_analyser.add(ResourceOverlapAnalyser('data_gov'))

    a3 = res_analyser.add(ResourceOccurrenceCountAnalyser('data_gov'))
    iter = Resource.iter(res)
    process_all(res_analyser, iter)

    rep1 = ResourceOverlapReporter(a1)
    rep2 = ResourceOverlapReporter(a2)
    print 'Num of multiple dataset occurrences'
    rep3 = ElementCountReporter(a3, columns=['Occurrences','Count'])

    #r = Report([rep1, rep2])
    print rep1.getDataFrame()
    print rep2.getDataFrame()
    print rep3.getDataFrame()

    exit()

    pmd_analyser = AnalyserSet()

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
    bins = [0,100,500,1000,10000,50000,100000,1000000,10000000]
    ds_histogram = pmd_analyser.add(PMDDatasetCountAnalyser(bins=bins))
    res_histogram = pmd_analyser.add(PMDResourceCountAnalyser(bins=bins))

    # 3. total num of values in url field (num of resources) vs unique and valid urls
    # 4. Portal Overlap: num of unique resources more then once -> datasets in same portal vs different portals
    # TODO resource analyser



    # 5. num of overlapping resources in pan european portal
    # resources, unique resources
    # TODO
    # analyser #1: argument filter by portal
    #- total urls
    #- cross portal urls -> dict in 2, 3, 4, 5 portals
    #
    #- portal intern urls


    # 6. extra keys in one, resp. more than one, more than two, more than x portals
    res_key_count = pmd_analyser.add(CKANKeysCount(keys_set='res', total_count=False))


    # 7. same for tags
    # TODO

    # retrievability
    retr_distr = pmd_analyser.add(DatasetStatusCount())
    # TODO resource resp code distribution


    #np.arange(0,1,0.1)

    bins=np.arange(0.0,1.1,0.1)
    compl_histogram = pmd_analyser.add(CompletenessHistogram(bins=bins))
    usage_histogram = pmd_analyser.add(UsageHistogram(bins=bins))
    cont_histogram = pmd_analyser.add(ContactabilityHistogram(bins=bins))
    open_histogram = pmd_analyser.add(OpennessHistogram(bins=bins))

    pmds = dbm.getPortalMetaDatasBySoftware(software='CKAN', snapshot=sn)
    pmd_iter = PortalMetaData.iter(pmds)
    process_all(pmd_analyser, pmd_iter)

    ################# RESULTS ###########################
    print 'ds_count', ds_count.getResult()
    print 'res_count', res_count.getResult()
    print 'license ids', len(lid_count.getResult())
    print 'file formats', len(format_count.getResult())
    print 'tags', len(tags_count.getResult())
    print 'keys', len(key_count.getResult())
    print 'core', len(core_key_count.getResult())
    print 'extra', len(extra_key_count.getResult())
    print 'res', len(res_key_count.getResult())

    # top k reporter
    format_rep = FormatCountReporter(format_count, topK=10)
    tags_rep = TagReporter(tags_count, ds_count, topK=10)
    key_rep = ElementCountReporter(key_count, columns=['Keys', 'Count'])
    core_rep = ElementCountReporter(core_key_count, columns=['Core Keys', 'Count'])
    extra_rep = ElementCountReporter(extra_key_count, columns=['Extra Keys', 'Count'])
    res_rep = ElementCountReporter(res_key_count, columns=['Resource Keys', 'Count'])
    lid_rep = ElementCountReporter(lid_count, columns=['License ID', 'Count'])
    retr_rep = ElementCountReporter(retr_distr, columns=['Retrievable', 'Count'])
    csv_re = Report([format_rep, tags_rep, key_rep, extra_rep, res_rep, lid_rep, retr_rep])
    csv_re.csvreport('tmp')

    print 'ds_histogram', ds_histogram.getResult()
    print 'res_histogram', res_histogram.getResult()



    # histogram reporting
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
    data = OrderedDict([(g, (compl_histogram.data[g], usage_histogram.data[g])) for g in ['total', 'res', 'core', 'extra']])
    colors = ['black', 'red', 'blue', 'green']
    scatter = MultiScatterReporter(data, keys_labels, "$Q_c$", "$Q_u$", "cvude.pdf", colors=colors)

    re = Report([con_rep, compl_rep, open_rep, scatter])
    re.plotreport('tmp')
