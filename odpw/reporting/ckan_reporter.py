from collections import OrderedDict
import pprint
import operator
import os
import numpy as np
import pandas as pd
import pickle
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.compare_analyser import CKANKeyIntersectionAnalyser, OGDMetadatenAnalyser
from odpw.analysers.core import ElementCountAnalyser, HistogramAnalyser, StatusCodeAnalyser
from odpw.analysers.count_analysers import DatasetCount, ResourceCount, CKANFormatCount, CKANTagsCount, CKANKeysCount, \
    CKANLicenseIDCount
from odpw.analysers.fetching import CKANKeyAnalyser, UsageAnalyser
from odpw.analysers.pmd_analysers import PMDDatasetCountAnalyser, PMDResourceCountAnalyser, CompletenessHistogram, \
    ContactabilityHistogram, OpennessHistogram, UsageHistogram, AccuracyHistogram, FormatHistogram, FormatDistribution
from odpw.analysers.resource_analysers import ResourceOverlapAnalyser, ResourceOccurrenceCountAnalyser
from odpw.analysers.statuscodes import DatasetStatusCount, ResourceStatusCode
from odpw.db.dbm import PostgressDBM
from odpw.db.models import Dataset, PortalMetaData, Resource, Portal
from odpw.reporting import plotting
from odpw.reporting.reporters import Report, Reporter, PlotReporter, FormatCountReporter, TagReporter, \
    ElementCountReporter, CSVReporter, ResourceOverlapReporter

from matplotlib.ticker import FuncFormatter
from matplotlib import pyplot as plt
from _collections import defaultdict


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


class MultiScatterReporter(Reporter, PlotReporter):
    def __init__(self, data, labels, xlabel, ylabel, filename, colors=None):
        super(MultiScatterReporter, self).__init__()
        self.data = data
        self.labels = labels
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.filename = filename
        self.colors = colors

    def plotreport(self, dir):
        plotting.scatterplotComb(self.data, self.labels, self.xlabel, self.ylabel, dir, self.filename, colors=self.colors)

class MultiHistogramReporter(Reporter, PlotReporter):
    def __init__(self, data, labels, xlabel, ylabel, filename, bins=None, colors=None, legend='upper right'):
        super(MultiHistogramReporter, self).__init__()
        self.data = data
        self.labels = labels
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.filename = filename
        self.bins = bins
        self.colors = colors
        self.legend = legend

    def plotreport(self, dir):
        plotting.histplotComp(self.data, self.labels, self.xlabel, self.ylabel, dir, self.filename, bins=self.bins, colors=self.colors, legend=self.legend)

class HistogramReporter(Reporter, PlotReporter):
    def __init__(self, data, xlabel, ylabel, filename, bins=None, color=None):
        super(HistogramReporter, self).__init__()
        self.data = data
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.filename = filename
        self.bins = bins
        self.color = color

    def plotreport(self, dir):
        plotting.histplot(self.data, self.xlabel, self.ylabel, dir, self.filename, bins=self.bins, color=self.color)


def overlap_report(dbm, sn, portals=None, portal_filter=None):
    res_analyser = AnalyserSet()
    overlap = res_analyser.add(ResourceOverlapAnalyser(portal_filter))
    mult_occur = res_analyser.add(ResourceOccurrenceCountAnalyser())
    if portals:
        for p in portals:
            process_all(res_analyser, Resource.iter(dbm.getResources(snapshot=sn, portalID=p)))
    else:
        process_all(res_analyser, Resource.iter(dbm.getResources(snapshot=sn)))

    occur_dict = mult_occur.getResult()
    print 'mult occurences:', sum(occur_dict[k] for k in occur_dict if k != 1)
    single_dict = mult_occur.getSinglePortalOccurences()
    print 'single occurences:', sum(single_dict[k] for k in single_dict if k != 1)

    overlap_dict = overlap.getResult()
    max_overlap = defaultdict(int)
    for p_source in overlap_dict:
        for p_dest in overlap_dict[p_source]:
            if p_source != p_dest:
                max_overlap[p_source] += overlap_dict[p_source][p_dest]

    sorted_overlap = sorted(max_overlap.items(), key=operator.itemgetter(1), reverse=True)
    print 'max overlap portal:', sorted_overlap[0]
    print '2nd overlap portal:', sorted_overlap[1]
    print '3rd overlap portal:', sorted_overlap[2]
    print 'summed up:', sum(v[1] for v in sorted_overlap)

    rep1 = ResourceOverlapReporter(overlap)
    rep2 = ElementCountReporter(mult_occur, columns=['Occurrences', 'Count'])

    return Report([rep1, rep2])

def key_report(dbm, sn):
    pmd_analyser = AnalyserSet()

    # 6. extra keys in one, resp. more than one, more than two, more than x portals

    res_key_count = pmd_analyser.add(CKANKeysCount(keys_set='res', total_count=False))

    key_count = pmd_analyser.add(CKANKeysCount(total_count=False))
    core_key_count = pmd_analyser.add(CKANKeysCount(keys_set='core', total_count=False))
    extra_key_count = pmd_analyser.add(CKANKeysCount(keys_set='extra', total_count=False))
    pmds = dbm.getPortalMetaDatasBySoftware(software='CKAN', snapshot=sn)
    pmd_iter = PortalMetaData.iter(pmds)
    process_all(pmd_analyser, pmd_iter)

    keys = extra_key_count.getResult()
    print 'res keys', len(res_key_count.getResult())
    print 'extra keys', len(keys)
    print 'extra keys in one portal', len([k for k in keys if keys[k] == 1])
    print 'extra keys in more than one portal', len([k for k in keys if keys[k] > 1])
    print 'extra keys in more than 2 portal', len([k for k in keys if keys[k] > 2])
    print 'extra keys in more than 20 portal', len([k for k in keys if keys[k] > 20])
    sorted_keys = sorted(keys.items(), key=operator.itemgetter(1), reverse=True)
    print 'max extra keys:', sorted_keys[0]
    print '2nd max extra keys:', sorted_keys[1]
    print '3nd max extra keys:', sorted_keys[2]

    print 'core', len(core_key_count.getResult())
    print 'res', len(res_key_count.getResult())

    key_rep = ElementCountReporter(key_count, columns=['Keys', 'Count'])
    core_rep = ElementCountReporter(core_key_count, columns=['Core Keys', 'Count'])
    extra_rep = ElementCountReporter(extra_key_count, columns=['Extra Keys', 'Count'])
    res_rep = ElementCountReporter(res_key_count, columns=['Resource Keys', 'Count'])

    report = Report([key_rep, core_rep, extra_rep, res_rep])
    return report

def res_retr_report(dbm, sn):
    res_an = AnalyserSet()
    res_count = res_an.add(ResourceCount())
    res_retr_distr = res_an.add(ResourceStatusCode())

    res = dbm.getResources(snapshot=sn)
    res_iter = Resource.iter(res)
    process_all(res_an, res_iter)

    print 'total res', res_count.getResult()
    res_retr_rep = ElementCountReporter(res_retr_distr, columns=['Retrievable', 'Count'])
    re = Report([res_retr_rep])
    return re


def retr_report(dbm, sn):
    pmd_analyser = AnalyserSet()
    # retrievability
    ds_count = pmd_analyser.add(DatasetCount())
    retr_distr = pmd_analyser.add(DatasetStatusCount())

    pmds = dbm.getPortalMetaDatasBySoftware(software='CKAN', snapshot=sn)
    pmd_iter = PortalMetaData.iter(pmds)
    process_all(pmd_analyser, pmd_iter)

    print 'total ds', ds_count.getResult()

    retr_rep = ElementCountReporter(retr_distr, columns=['Retrievable', 'Count'])
    re = Report([retr_rep])

    return re


def tag_report(dbm, sn):
    pmd_analyser = AnalyserSet()

    tags_count = pmd_analyser.add(CKANTagsCount(total_count=False))

    pmds = dbm.getPortalMetaDatasBySoftware(software='CKAN', snapshot=sn)
    pmd_iter = PortalMetaData.iter(pmds)
    process_all(pmd_analyser, pmd_iter)

    tags = tags_count.getResult()

    print 'tags in one portal', len([k for k in tags if tags[k] == 1])
    print 'tags in more than one portal', len([k for k in tags if tags[k] > 1])
    print 'tags in more than 2 portal', len([k for k in tags if tags[k] > 2])
    print 'tags in more than 35 portal', len([k for k in tags if tags[k] > 35])

    #sorted_keys = sorted(tags.items(), key=operator.itemgetter(1), reverse=True)

#    report = Report([key_rep, core_rep, extra_rep, res_rep])
#    return report



def obd_report(dbm, sn):
    pmd_analyser = AnalyserSet()

    # 1. STATS
    ds_count = pmd_analyser.add(DatasetCount())
    res_count = pmd_analyser.add(ResourceCount())
    format_count = pmd_analyser.add(CKANFormatCount())
    tags_count = pmd_analyser.add(CKANTagsCount())


    lid_count = pmd_analyser.add(CKANLicenseIDCount(total_count=True))

    # 2. Portal size distribution
    bins = [0,100,500,1000,10000,50000,100000,1000000,10000000]
    ds_histogram = pmd_analyser.add(PMDDatasetCountAnalyser(bins=bins))
    res_histogram = pmd_analyser.add(PMDResourceCountAnalyser(bins=bins))

    # 3. total num of values in url field (num of resources) vs unique and valid urls
    # 4. Portal Overlap: num of unique resources more then once -> datasets in same portal vs different portals
    # TODO resource analyser



    # 7. same for tags
    # TODO

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

    # top k reporter
    format_rep = FormatCountReporter(format_count, topK=10)
    tags_rep = TagReporter(tags_count, ds_count, topK=10)
    lid_rep = ElementCountReporter(lid_count, columns=['License ID', 'Count'])
    csv_re = Report([format_rep, tags_rep, lid_rep])
    csv_re.csvreport('tmp')

    print 'ds_histogram', ds_histogram.getResult()
    print 'res_histogram', res_histogram.getResult()



    # histogram reporting
    #col = ['black', 'white', 'dimgrey', 'lightgrey']
    col = ['black', 'red', 'blue', 'green']
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
    usage_rep = MultiHistogramReporter(data, labels=keys_labels, xlabel="$Q_u$", ylabel=ylabel, filename='qu.pdf', colors=col)

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
    scatter = MultiScatterReporter(data, keys_labels, "$Q_c$", "$Q_u$", "cvude.pdf", colors=col)

    re = Report([con_rep, compl_rep, open_rep, usage_rep, scatter])
    return re

def openness_report(dbm, sn):
    pmd_analyser = AnalyserSet()

    format_count = pmd_analyser.add(CKANFormatCount())
    lid_count = pmd_analyser.add(CKANLicenseIDCount(total_count=False))

    pmds = dbm.getPortalMetaDatasBySoftware(software='CKAN', snapshot=sn)
    pmd_iter = PortalMetaData.iter(pmds)
    process_all(pmd_analyser, pmd_iter)

    format_rep = FormatCountReporter(format_count,topK=500)
    lid_rep = ElementCountReporter(lid_count, columns=['License ID', 'Count'])

    re = Report([lid_rep, format_rep])
    return re

def accuracy_report(dbm):
    path = 'tmp/accuracy/accr.pkl'

    accr_res = []
    with open(path, 'r') as f:
        accr_res.append(pickle.load(f))

    col = ['black', 'red', 'blue', 'green']
    portals = dbm.getPortals(software='CKAN')
    for p in Portal.iter(portals):
        pkl_file = path + p.id + '.pkl'
        if os.path.exists(pkl_file):
            with open(pkl_file, 'r') as f:
                accr_res.append(pickle.load(f))

    analyser = AnalyserSet()
    bins = np.arange(0.0, 1.1, 0.1)
    histogram = analyser.add(AccuracyHistogram(bins=bins))
    process_all(analyser, accr_res)


    counts = histogram.getCounts()

    print 'qaf:', sum(counts['qaf']), len(counts['qaf'])
    print 'qam:', sum(counts['qam']), len(counts['qam'])
    print 'qas:', sum(counts['qas']), len(counts['qas'])

    labels = {'format': "$Q_a (format)$", 'mime_type': "$Q_a (mime\_type)$", 'size': "$Q_a (size)$"}

    res = histogram.getResult()
    data = OrderedDict([('format', res['qaf']), ('mime_type', res['qam']), ('size', res['qas'])])

    rep = MultiHistogramReporter(data, labels=labels, xlabel="$Q_a$", ylabel="Portals", filename="qa.pdf", colors=col)
    return Report([rep])


def intersec_report(dbm, sn, p_id):

    analyser = AnalyserSet()

    pmd = dbm.getPortalMetaData(portalID=p_id, snapshot=sn)
    ka = analyser.add(CKANKeyAnalyser())

    ds = dbm.getDatasets(portalID=p_id, snapshot=sn)
    process_all(analyser, Dataset.iter(ds))

    ka.update(pmd)

    # inter = analyser.add(CKANKeyIntersectionAnalyser())
    ogd = analyser.add(OGDMetadatenAnalyser())

    process_all(analyser, [pmd])

#    dict = inter.getResult()

#    print 'total', dict['total']
#    print 'intersection', dict['intersection']
#    print 'sym_diff', dict['sym_diff']

    pprint.pprint(ogd.getResult())


def format_dist_report(dbm, sn):

    analyser = AnalyserSet()

    pmd = dbm.getPortalMetaDatas(snapshot=sn)

    formats = ['CSV', 'PDF', 'HTML']
    form_hist = analyser.add(FormatHistogram(formats))

    #bins = np.linspace(0, 1000000, 10)
    form_dist = analyser.add(FormatDistribution(formats))

    process_all(analyser, PortalMetaData.iter(pmd))


    col = ['black', 'red', 'blue', 'green', 'yellow']
    labels = {f: f for f in formats}
    res = form_hist.getResult()
    data = OrderedDict([(f, res[f]) for f in formats])
    rep = MultiHistogramReporter(data, labels=labels, xlabel="Ratio of specified format", ylabel="Portals", filename='formats_hist.pdf', colors=col)

    res2 = form_dist.getResult()
    data2 = OrderedDict([(f, res2[f]) for f in formats])
    dist_rep = MultiHistogramReporter(data2, bins=data2['CSV']['bin_edges'], legend='upper left', labels=labels, xlabel="Number of Datasets on Portal", ylabel="Resources", filename='formats_dist.pdf', colors=col)

    re = Report([rep, dist_rep])
    return re


if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    sn = 1533
    aut_portals = [p.id for p in Portal.iter(dbm.getPortals(iso3='AUT'))]
    report = overlap_report(dbm, sn, portals=aut_portals)

    report.csvreport('tmp/overlap')
