from collections import OrderedDict
import numpy as np
from odpw.analysers import AnalyserSet, process_all, SAFEAnalyserSet
from odpw.analysers.core import HistogramAnalyser, DCATConverter
from odpw.analysers.count_analysers import DatasetCount, DCATFormatCount, DCATTagsCount
from odpw.analysers.pmd_analysers import PMDDatasetCountAnalyser
from odpw.analysers.quality.new.conformance_dcat import *
from odpw.analysers.quality.new.existence_dcat import *
from odpw.analysers.quality.new.open_dcat_format import IANAFormatDCATAnalyser, FormatOpennessDCATAnalyser, \
    FormatMachineReadableDCATAnalyser
from odpw.analysers.quality.new.open_dcat_license import LicenseOpennessDCATAnalyser
from odpw.db.dbm import PostgressDBM
from odpw.db.models import PortalMetaData, Dataset
from odpw.reporting.plot_reporter import MultiHistogramReporter, MultiScatterReporter
from odpw.reporting.reporters import FormatCountReporter, TagReporter, Report

__author__ = 'sebastian'

def general_stats(dbm, sn):
    pmd_analyser = AnalyserSet()

    # 1. STATS
    ds_count = pmd_analyser.add(DatasetCount())
    format_count = pmd_analyser.add(DCATFormatCount())
    tags_count = pmd_analyser.add(DCATTagsCount())

    # 2. Portal size distribution
    bins = [0,50,100,500,1000,10000,100000,10000000]
    ds_histogram = pmd_analyser.add(PMDDatasetCountAnalyser(bins=bins))

    pmds = dbm.getPortalMetaDatas(snapshot=sn)
    pmd_iter = PortalMetaData.iter(pmds)
    process_all(pmd_analyser, pmd_iter)

    ################# RESULTS ###########################
    print 'ds_count', ds_count.getResult()
    print 'file formats', len(format_count.getResult())
    print 'tags', len(tags_count.getResult())

    # top k reporter
    format_rep = FormatCountReporter(format_count, topK=10)
    tags_rep = TagReporter(tags_count, ds_count, topK=10)
    csv_re = Report([format_rep, tags_rep])
    csv_re.csvreport('tmp')

    print 'ds_histogram', ds_histogram.getResult()


def calculateMetrics(dbm, sn, p_id):
    p = dbm.getPortal(portalID=p_id)
    analyser = AnalyserSet()
    analyser.add(DCATConverter(p))

    ##################### EXISTS ######################################
    # ACCESS
    access = analyser.add(AnyMetric([AccessUrlDCAT(), DownloadUrlDCAT()], id='ExAc'))
    # DISCOVERY
    discovery = analyser.add(AverageMetric([DatasetTitleDCAT(), DatasetDescriptionDCAT(), DatasetKeywordsDCAT(), DistributionTitleDCAT(), DistributionDescriptionDCAT()], id='ExDi'))
    # CONTACT
    contact = analyser.add(AnyMetric([DatasetContactDCAT(), DatasetPublisherDCAT()], id='ExCo'))
    # LICENSE
    rights = analyser.add(ProvLicenseDCAT())
    # PRESERVATION
    preservation = analyser.add(AverageMetric([DatasetAccrualPeriodicityDCAT(), DistributionFormatsDCAT(), DistributionMediaTypesDCAT(), DistributionByteSizeDCAT()], id='ExPr'))
    # DATE
    date = analyser.add(AverageMetric([DatasetCreationDCAT(), DatasetModificationDCAT(), DistributionIssuedDCAT(), DistributionModifiedDCAT()], id='ExDa'))
    # TEMPORAL
    temporal = analyser.add(DatasetTemporalDCAT())
    # SPATIAL
    spatial = analyser.add(DatasetSpatialDCAT())

    ####################### CONFORMANCE ###########################
    # ACCESS
    accessUri = analyser.add(AnyConformMetric([ConformAccessUrlDCAT(), ConformDownloadUrlDCAT()], id='CoAc'))
    # CONTACT
    contactEmail = analyser.add(AnyConformMetric([EmailConformContactPoint(), EmailConformPublisher()], id='CoCE'))
    contactUri = analyser.add(AnyConformMetric([UrlConformContactPoint(), UrlConformPublisher()], id='CoCU'))

    # DATE
    dateformat = analyser.add(AverageConformMetric([DateConform(dcat_access.getCreationDate),
                                             DateConform(dcat_access.getModificationDate),
                                             DateConform(dcat_access.getDistributionCreationDates),
                                             DateConform(dcat_access.getDistributionModificationDates)], id='CoDa'))
    # LICENSE
    licenseConf = analyser.add(LicenseConform())
    # FORMAT
    formatConf = analyser.add(IANAFormatDCATAnalyser())

    ####################### OPENNESS ###########################
    formatOpen = analyser.add(FormatOpennessDCATAnalyser())
    formatMachine = analyser.add(FormatMachineReadableDCATAnalyser())
    licenseOpen = analyser.add(LicenseOpennessDCATAnalyser())

    ds = dbm.getDatasets(snapshot=sn, portalID=p_id)
    d_iter = Dataset.iter(ds)
    process_all(analyser, d_iter)

    pmd = dbm.getPortalMetaData(portalID=p_id, snapshot=sn)

    analyser.update(pmd)
    #dbm.updatePortalMetaData(pmd)

    return {
        'Access': access.getValue(),
        'Discovery': discovery.getValue(),
        'Contact': contact.getValue(),
        'Rights': rights.getResult(),
        'Preservation': preservation.getValue(),
        'Date': date.getValue(),
        'Temporal': temporal.getResult(),
        'Spatial': spatial.getResult(),
        'AccessURI': accessUri.getValue(),
        'ContactEmail': contactEmail.getValue(),
        'ContactURI': contactUri.getValue(),
        'DateFormat': dateformat.getValue(),
        'License': licenseConf.getValue(),
        'FileFormat': formatConf.getResult()[IANAFormatDCATAnalyser.id],
        'OpenFormat': formatOpen.getResult()[FormatOpennessDCATAnalyser.id],
        'MachineRead': formatMachine.getResult()[FormatMachineReadableDCATAnalyser.id],
        'OpenLicense': licenseOpen.getResult()
    }



def exists_report(dbm, sn, metrics):
    conf_values = {}
    with open('tmp/test_portals.txt') as f:
        for p_id in f:
            v = calculateMetrics(dbm, sn, p_id.strip())
            conf_values[p_id] = v
    col = ['black', 'red', 'blue', 'green']
    keys_labels = {}
    xlabel = "Existence"
    ylabel = "Portals"
    filename = "exist.pdf"

    bins = np.arange(0.0, 1.1, 0.1)
    data = OrderedDict()
    for m in metrics:
        keys_labels[m] = m
        hist, bin_edges = np.histogram(np.array([conf_values[d][m] for d in conf_values]), bins=bins)
        data[m] = {'hist': hist, 'bin_edges': bin_edges}
    rep = MultiHistogramReporter(data, labels=keys_labels, xlabel=xlabel, ylabel=ylabel, filename=filename, colors=col)
    re = Report([rep])
    re.plotreport('tmp')


def conform_report(dbm, sn, metrics):
    conf_values = {}
    with open('tmp/test_portals.txt') as f:
        for p_id in f:
            v = calculateMetrics(dbm, sn, p_id.strip())
            conf_values[p_id] = v
    col = ['black', 'red', 'blue', 'green', 'grey']
    keys_labels = {}
    xlabel = "Conformance"
    ylabel = "Portals"
    filename = "conf.pdf"

    bins = np.arange(0.0, 1.1, 0.1)
    data = OrderedDict()
    for m in metrics:
        keys_labels[m] = m
        hist, bin_edges = np.histogram(np.array([conf_values[d][m] for d in conf_values]), bins=bins)
        data[m] = {'hist': hist, 'bin_edges': bin_edges}
    rep = MultiHistogramReporter(data, labels=keys_labels, xlabel=xlabel, ylabel=ylabel, filename=filename, colors=col)
    re = Report([rep])
    re.plotreport('tmp')

def scatter_report(dbm, sn):
    metrics = ['Rights' 'License']
    values = {}
    with open('tmp/test_portals.txt') as f:
        for p_id in f:
            v = calculateMetrics(dbm, sn, p_id.strip())
            values[p_id] = v
    col = ['black', 'red', 'blue', 'green', 'grey']
    keys_labels = {}

    bins = np.arange(0.0, 1.1, 0.1)
    data = OrderedDict()
    for m in metrics:
        keys_labels[m] = m
        hist, bin_edges = np.histogram(np.array([values[d][m] for d in values]), bins=bins)
        data[m] = {'hist': hist, 'bin_edges': bin_edges}

    #data = OrderedDict([(g, ()) for g in ['total', 'res', 'core', 'extra']])
    #scatter = MultiScatterReporter(data, keys_labels, "$Q_c$", "$Q_u$", "cvude.pdf", colors=col)



if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    sn = 1542

    #metrics = ['DateFormat', 'License', 'FileFormat']

    #conform_report(dbm, sn, metrics)

    #calculateMetrics(dbm, sn, 'data_gv_at')

    with open('tmp/test_portals.txt') as f:
        for p_id in f:
            v = calculateMetrics(dbm, sn, p_id.strip())
