from collections import OrderedDict
import numpy as np
from odpw.analysers import AnalyserSet, process_all, SAFEAnalyserSet
from odpw.analysers.core import HistogramAnalyser, DCATConverter
from odpw.analysers.count_analysers import DatasetCount, DCATFormatCount, DCATTagsCount, DCATLicenseCount, ResourceCount
from odpw.analysers.pmd_analysers import PMDDatasetCountAnalyser, MultiHistogramAnalyser
from odpw.analysers.quality.new.conformance_dcat import *
from odpw.analysers.quality.new.existence_dcat import *
from odpw.analysers.quality.new.open_dcat_format import IANAFormatDCATAnalyser, FormatOpennessDCATAnalyser, \
    FormatMachineReadableDCATAnalyser
from odpw.analysers.quality.new.open_dcat_license import LicenseOpennessDCATAnalyser
from odpw.analysers.statuscodes import DatasetStatusCode, ResourceStatusCode
from odpw.db.dbm import PostgressDBM
from odpw.db.models import PortalMetaData, Dataset, Portal
from odpw.reporting.plot_reporter import MultiHistogramReporter, MultiScatterReporter, MultiScatterHistReporter
from odpw.reporting.reporters import FormatCountReporter, TagReporter, Report, LicenseCountReporter, \
    ElementCountReporter

__author__ = 'sebastian'


class PMDDCATMetricAnalyser(HistogramAnalyser):
    def __init__(self, id, **nphistparams):
        super(PMDDCATMetricAnalyser, self).__init__(**nphistparams)
        self.id = id

    def analyse_PortalMetaData(self, pmd):
        if pmd.qa_stats and self.id in pmd.qa_stats:
            self.analyse_generic(pmd.qa_stats[self.id])

    def name(self):
        return self.id

class PMDDCATSoftwareAnalyser(MultiHistogramAnalyser):
    def __init__(self, id, dbm, **nphistparams):
        super(PMDDCATSoftwareAnalyser, self).__init__(**nphistparams)
        self.id = id
        self.dbm = dbm

    def analyse_PortalMetaData(self, pmd):
        if pmd.qa_stats and self.id in pmd.qa_stats:
            quality = pmd.qa_stats[self.id]
            p = dbm.getPortal(portalID=pmd.portal_id)
            self.data[p.software].append(quality)

    def name(self):
        return self.id


def general_stats(dbm, sn):
    pmd_analyser = AnalyserSet()

    # 1. STATS
    ds_count = pmd_analyser.add(DatasetCount())
    format_count = pmd_analyser.add(DCATFormatCount(total_count=False))
    licenses_count = pmd_analyser.add(DCATLicenseCount(total_count=False))
    #tags_count = pmd_analyser.add(DCATTagsCount())

    # 2. Portal size distribution
    bins = [0,50,100,500,1000,5000,10000,50000,100000,10000000]
    ds_histogram = pmd_analyser.add(PMDDatasetCountAnalyser(bins=bins))

    pmds = dbm.getPortalMetaDatas(snapshot=sn)
    pmd_iter = PortalMetaData.iter(pmds)
    process_all(pmd_analyser, pmd_iter)

    ################# RESULTS ###########################
    print 'ds_count', ds_count.getResult()
    print 'file formats', len(format_count.getResult())
    print 'licenses', len(licenses_count.getResult())
    #print 'tags', len(tags_count.getResult())

    # top k reporter
    format_rep = FormatCountReporter(format_count, topK=800)
    license_rep = LicenseCountReporter(licenses_count, topK=100)
    #tags_rep = TagReporter(tags_count, ds_count, topK=10)
    csv_re = Report([format_rep, license_rep])
    csv_re.csvreport('tmp')

    print 'ds_histogram', ds_histogram.getResult()

def calculateMetrics(dbm, sn, p):
    analyser = SAFEAnalyserSet()
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


    ############## Iterate DS ################################
    ds = dbm.getDatasetsAsStream(snapshot=sn, portalID=p.id)
    d_iter = Dataset.iter(ds)
    process_all(analyser, d_iter)

    ############# Update PMD #################################
    pmd = dbm.getPortalMetaData(portalID=p.id, snapshot=sn)
    analyser.update(pmd)
    dbm.updatePortalMetaData(pmd)

    values = {
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
    return values


col = ['#7fc97f', '#fdc086', '#386cb0', '#beaed4', '#ffff99']
#col = ['#a6611a','#dfc27d','#80cdc1','#018571']

keys_labels = {
        'ExAc': '\\textsf{Access}',
        'ExDi': '\\textsf{Discovery}',
        'ExCo': '\\textsf{Contact}',
        'ExRi': '\\textsf{Rights}',
        'ExPr': '\\textsf{Preservation}',
        'ExDa': '\\textsf{Date}',
        'ExTe': '\\textsf{Temporal}',
        'ExSp': '\\textsf{Spatial}',
        'CoAc': '\\textsf{AccessURI}',
        'CoCE': '\\textsf{ContactEmail}',
        'CoCU': '\\textsf{ContactURI}',
        'CoDa': '\\textsf{DateFormat}',
        'CoLi': '\\textsf{License}',
        'CoFo': '\\textsf{FileFormat}',
        'OpFo': '\\textsf{OpenFormat}',
        'OpMa': '\\textsf{MachineRead}',
        'OpLi': '\\textsf{OpenLicense}'
}

def getMetrics(dbm, sn, metrics, bins):
    aSet = AnalyserSet()
    analyser = {}
    for id in metrics:
        analyser[id] = aSet.add(PMDDCATMetricAnalyser(id, bins=bins))

    pmds = dbm.getPortalMetaDatas(snapshot=sn)
    pmd_iter = PortalMetaData.iter(pmds)
    process_all(aSet, pmd_iter)
    return analyser


def getMetricsBySoftware(dbm, sn, metrics, bins):
    aSet = AnalyserSet()
    analyser = {}
    for id in metrics:
        analyser[id] = aSet.add(PMDDCATSoftwareAnalyser(id, dbm, bins=bins))

    pmds = dbm.getPortalMetaDatas(snapshot=sn)
    pmd_iter = PortalMetaData.iter(pmds)
    process_all(aSet, pmd_iter)
    return analyser

def exists_report(dbm, sn, metrics):
    bins = np.arange(0.0, 1.1, 0.1)
    values = getMetrics(dbm, sn, metrics, bins)

    xlabel = "\\textsc{Existence}"
    ylabel = "Portals"
    filename = "exist.pdf"

    data = OrderedDict()
    for m in metrics:
        data[m] = values[m].getResult()
    rep = MultiHistogramReporter(data, labels=keys_labels, xlabel=xlabel, ylabel=ylabel, filename=filename, colors=col)
    re = Report([rep])
    re.plotreport('tmp')


def conform_report(dbm, sn, metrics):
    bins = np.arange(0.0, 1.1, 0.1)
    values = getMetrics(dbm, sn, metrics, bins)
    xlabel = "\\textsc{Conformance}"
    ylabel = "Portals"
    filename = "conf.pdf"

    data = OrderedDict()
    for m in metrics:
        data[m] = values[m].getResult()
    rep = MultiHistogramReporter(data, labels=keys_labels, xlabel=xlabel, ylabel=ylabel, filename=filename, colors=col)
    re = Report([rep])
    re.plotreport('tmp')

def open_report(dbm, sn, metrics):
    bins = np.arange(0.0, 1.1, 0.1)
    values = getMetrics(dbm, sn, metrics, bins)
    xlabel = "\\textsc{Open Data}"
    ylabel = "Portals"
    filename = "open.pdf"

    data = OrderedDict()
    for m in metrics:
        data[m] = values[m].getResult()
    rep = MultiHistogramReporter(data, labels=keys_labels, xlabel=xlabel, ylabel=ylabel, filename=filename, colors=col)
    re = Report([rep])
    re.plotreport('tmp')

def scatter_report(dbm, sn, xMetric, yMetric, xlabel="Existence", ylabel="Conformance", filename="rights.pdf"):
    softw_col = ['#ca0020', '#f4a582', '#0571b0', '#92c5de']
    bins = np.arange(0.0, 1.1, 0.1)
    values = getMetricsBySoftware(dbm, sn, [xMetric, yMetric], bins)

    labels = OrderedDict()

    data = OrderedDict()
    hist_data = OrderedDict()
    for s in ['Socrata', 'OpenDataSoft','CKAN']:
        data[s] = (values[xMetric].data[s], values[yMetric].data[s])
        for m in [xMetric, yMetric]:
            hist_data[m] = values[m].getResult()
        labels[s] = s

    scatter = MultiScatterHistReporter(data, hist_data, bins, labels, xlabel, ylabel, filename, colors=softw_col)
    re = Report([scatter])
    re.plotreport('tmp')

def retr_report(dbm, sn):
    pmd_analyser = AnalyserSet()
    # retrievability
#    ds_count = pmd_analyser.add(DatasetCount())
    res_count = pmd_analyser.add(ResourceCount())
    retr_distr = pmd_analyser.add(DatasetStatusCode())
#    res_retr_distr = pmd_analyser.add(ResourceStatusCode())

    pmds = dbm.getPortalMetaDatas(snapshot=sn)
    pmd_iter = PortalMetaData.iter(pmds)
    process_all(pmd_analyser, pmd_iter)

    print 'total res', res_count.getResult()

    retr_rep = ElementCountReporter(retr_distr, columns=['Retrievable', 'Count'])
    re = Report([retr_rep])

    return re

def calculate_license_count(dbm, sn):
    portals = [p for p in Portal.iter(dbm.getPortals())]
    for p in portals:
        try:
            print 'SNAPSHOT:', sn, 'PORTAL:', p.id
            analyser = SAFEAnalyserSet()
            analyser.add(DCATConverter(p))
            l = analyser.add(DCATLicenseCount())

            ############## Iterate DS ################################
            ds = dbm.getDatasetsAsStream(snapshot=sn, portalID=p.id)
            d_iter = Dataset.iter(ds)
            process_all(analyser, d_iter)

            ############# Update PMD #################################
            pmd = dbm.getPortalMetaData(portalID=p.id, snapshot=sn)
            analyser.update(pmd)
            dbm.updatePortalMetaData(pmd)
        except Exception as e:
            print 'EXCEPTION', e


if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    sn = 1542

    #general_stats(dbm, sn)
    #re = retr_report(dbm, sn)
    #re.csvreport('tmp')

    #metrics = ['ExAc', 'ExDi', 'ExCo']
    #exists_report(dbm, sn, ['ExPr', 'ExDa', 'ExTe', 'ExSp'])
    #exists_report(dbm, sn, ['ExAc', 'ExDi', 'ExCo', 'ExRi'])
    #conform_report(dbm, sn, ['CoAc', 'CoCE', 'CoCU'])
    #conform_report(dbm, sn, ['CoDa', 'CoLi', 'CoFo'])
    #open_report(dbm, sn, ['OpFo', 'OpMa', 'OpLi'])
    #scatter_report(dbm, sn, 'CoLi', 'OpLi', xlabel='Conformance', ylabel='Openness', filename='license_conf_open_scatter.pdf')

    scatter_report(dbm, sn, 'ExPr', 'OpFo', xlabel='\\textsf{Preservation}', ylabel='\\textsf{OpenFormat}', filename='sc_format_ex_op.pdf')
    scatter_report(dbm, sn, 'ExPr', 'CoFo', xlabel='\\textsf{Preservation}', ylabel='\\textsf{FileFormat}', filename='sc_format_ex_co.pdf')
    #scatter_report(dbm, sn, 'OpLi', 'CoLi', xlabel='Openness', ylabel='Conformance', filename='sc_license_co_op.pdf')
    #scatter_report(dbm, sn, 'ExPr', 'OpMa', xlabel='Existence', ylabel='Machine Readable', filename='sc_format_ex_ma.pdf')


    scatter_report(dbm, sn, 'ExPr', 'OpMa', ylabel='\\textsf{MachineRead}', xlabel='\\textsf{Preservation}', filename='sc_format_ex_ma.pdf')
    scatter_report(dbm, sn, 'ExCo', 'CoCE', xlabel='\\textsf{Contact}', ylabel='\\textsf{ContactEmail}', filename='sc_email_ex_co.pdf')
    scatter_report(dbm, sn, 'ExCo', 'CoCU', xlabel='\\textsf{Contact}', ylabel='\\textsf{ContactURL}', filename='sc_url_ex_co.pdf')

    #snapshots = xrange(1543, 1533, -1)
    #portals = [p for p in Portal.iter(dbm.getPortals())]
    #for sn in snapshots:
    #    for p in portals:
    #        print 'SNAPSHOT:', sn, 'PORTAL:', p.id
    #        v = calculateMetrics(dbm, sn, p)
