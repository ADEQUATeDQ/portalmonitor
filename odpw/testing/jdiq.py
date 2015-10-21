from collections import OrderedDict
import numpy as np
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.core import HistogramAnalyser, DCATConverter
from odpw.analysers.count_analysers import DatasetCount, DCATFormatCount, DCATTagsCount
from odpw.analysers.pmd_analysers import PMDDatasetCountAnalyser
from odpw.analysers.quality.new.conformance_dcat import *
from odpw.analysers.quality.new.existence_dcat import *
from odpw.db.dbm import PostgressDBM
from odpw.db.models import PortalMetaData, Dataset
from odpw.reporting.plot_reporter import MultiHistogramReporter
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


def conform(dbm, sn, p_id):
    analyser = AnalyserSet()
    p = dbm.getPortal(portalID=p_id)
    analyser.add(DCATConverter(p))
    ds_count = analyser.add(DatasetCount())

    # ACCESS
    anyAccess = analyser.add(AnyConformMetric([ConformAccessUrlDCAT(), ConformDownloadUrlDCAT()]))
    #conformaccessURL = analyser.add(ConformAccessUrlDCAT())
    #downloadURL = analyser.add(ConformDownloadUrlDCAT())

    # CONTACT
    anyEmail = analyser.add(AnyConformMetric([EmailConformContactPoint(), EmailConformPublisher()]))
    anyUrl = analyser.add(AnyConformMetric([UrlConformContactPoint(), UrlConformPublisher()]))

    # DATE
    dateformat = analyser.add(AverageConformMetric([DateConform(dcat_access.getCreationDate),
                                             DateConform(dcat_access.getModificationDate),
                                             DateConform(dcat_access.getDistributionCreationDates),
                                             DateConform(dcat_access.getDistributionModificationDates)]))

    # LICENSE
    license = analyser.add(LicenseConform())

    ds = dbm.getDatasets(snapshot=sn, portalID=p_id)
    d_iter = Dataset.iter(ds)
    process_all(analyser, d_iter)

    return {
        'AccessURI': anyAccess.getValue(),
        'ContactEmail': anyEmail.getValue(),
        'ContactURI': anyUrl.getValue(),
        'DateFormat': dateformat.getValue(),
        'LicenseID': license.getValue()
    }


def exists(dbm, sn, p_id):
    analyser = AnalyserSet()
    p = dbm.getPortal(portalID=p_id)
    analyser.add(DCATConverter(p))
    ds_count = analyser.add(DatasetCount())
    # ACCESS
    access = analyser.add(AnyMetric([AccessUrlDCAT(), DownloadUrlDCAT()]))
    #accessURL = analyser.add(AccessUrlDCAT())
    #downloadURL = analyser.add(DownloadUrlDCAT())
    # DISCOVERY
    discovery = analyser.add(AverageMetric([DatasetTitleDCAT(), DatasetDescriptionDCAT(), DatasetKeywordsDCAT(), DistributionTitleDCAT(), DistributionDescriptionDCAT()]))
    #dataset_title = analyser.add(DatasetTitleDCAT())
    #dataset_description = analyser.add(DatasetDescriptionDCAT())
    #dataset_keyword = analyser.add(DatasetKeywordsDCAT())
    #distr_title = analyser.add(DistributionTitleDCAT())
    #distr_description = analyser.add(DistributionDescriptionDCAT())
    # CONTACT
    contact = analyser.add(AnyMetric([DatasetContactDCAT(), DatasetPublisherDCAT()]))
    #concact_point = analyser.add(DatasetContactDCAT())
    #publisher = analyser.add(DatasetPublisherDCAT())
    # LICENSE
    license = analyser.add(ProvLicenseDCAT())
    # PRESERVATION
    preservation = analyser.add(AverageMetric([DatasetAccrualPeriodicityDCAT(), DistributionFormatsDCAT(), DistributionMediaTypesDCAT(), DistributionByteSizeDCAT()]))
    #accrual = analyser.add(DatasetAccrualPeriodicityDCAT())
    #format = analyser.add(DistributionFormatsDCAT())
    #mediaType = analyser.add(DistributionMediaTypesDCAT())
    #byteSize = analyser.add(DistributionByteSizeDCAT())
    # DATE
    date = analyser.add(AverageMetric([DatasetCreationDCAT(), DatasetModificationDCAT(), DistributionIssuedDCAT(), DistributionModifiedDCAT()]))
    #issued = analyser.add(DatasetCreationDCAT())
    #modified = analyser.add(DatasetModificationDCAT())
    #distr_issued = analyser.add(DistributionIssuedDCAT())
    #distr_modified = analyser.add(DistributionModifiedDCAT())
    # TEMPORAL
    temporal = analyser.add(DatasetTemporalDCAT())
    # SPATIAL
    spatial = analyser.add(DatasetSpatialDCAT())

    ds = dbm.getDatasets(snapshot=sn, portalID=p_id)
    d_iter = Dataset.iter(ds)
    process_all(analyser, d_iter)

    return {
        'access': access.getValue(),
        'discovery': discovery.getValue(),
        'contact': contact.getValue(),
        'license': license.getResult()['count']/ds_count.getResult()['count'] if ds_count.getResult()['count'] > 0 else 0,
        'preservation': preservation.getValue(),
        'date': date.getValue(),
        'temporal': temporal.getResult()['count']/ds_count.getResult()['count'] if ds_count.getResult()['count'] > 0 else 0,
        'spatial': spatial.getResult()['count']/ds_count.getResult()['count'] if ds_count.getResult()['count'] > 0 else 0
    }


def exists_report(dbm, sn, metrics):
    conf_values = {}
    with open('tmp/test_portals.txt') as f:
        for p_id in f:
            v = exists(dbm, sn, p_id.strip())
            conf_values[p_id] = v
    col = ['black', 'red', 'blue', 'green']
    keys_labels = {}
    xlabel = "Existence"
    ylabel = "Portals"
    filename = "exist.pdf"

    bins = np.arange(0.0, 1.1, 0.1)
    data = OrderedDict()
    for m in metrics:
        keys_labels[m] = m.title()
        hist, bin_edges = np.histogram(np.array([conf_values[d][m] for d in conf_values]), bins=bins)
        data[m] = {'hist': hist, 'bin_edges': bin_edges}
    rep = MultiHistogramReporter(data, labels=keys_labels, xlabel=xlabel, ylabel=ylabel, filename=filename, colors=col)
    re = Report([rep])
    re.plotreport('tmp')


def conform_report(dbm, sn, metrics):
    conf_values = {}
    with open('tmp/test_portals.txt') as f:
        for p_id in f:
            v = conform(dbm, sn, p_id.strip())
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


if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    sn = 1542

    #metrics = ['access', 'discovery', 'contact', 'license']
    metrics = ['AccessURI', 'ContactEmail', 'ContactURI', 'DateFormat', 'LicenseID']

    conform_report(dbm, sn, metrics)