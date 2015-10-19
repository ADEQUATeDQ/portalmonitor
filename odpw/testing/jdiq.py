from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.core import HistogramAnalyser, DCATConverter
from odpw.analysers.count_analysers import DatasetCount, DCATFormatCount, DCATTagsCount
from odpw.analysers.pmd_analysers import PMDDatasetCountAnalyser
from odpw.analysers.quality.new.existence_dcat import *
from odpw.db.dbm import PostgressDBM
from odpw.db.models import PortalMetaData, Dataset
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


def exists(dbm, sn):
    analyser = AnalyserSet()
    p_id = 'data_gv_at'
    p = dbm.getPortal(portalID=p_id)
    analyser.add(DCATConverter(p))
    ds_count = analyser.add(DatasetCount())
    # ACCESS
    accessURL = analyser.add(AccessUrlDCAT())
    downloadURL = analyser.add(DownloadUrlDCAT())
    # DISCOVERY
    dataset_title = analyser.add(DatasetTitleDCAT())
    dataset_description = analyser.add(DatasetDescriptionDCAT())
    dataset_keyword = analyser.add(DatasetKeywordsDCAT())
    distr_title = analyser.add(DistributionTitleDCAT())
    distr_description = analyser.add(DistributionDescriptionDCAT())
    # CONTACT
    concact_point = analyser.add(DatasetContactDCAT())
    publisher = analyser.add(DatasetPublisherDCAT())
    # LICENSE
    license = analyser.add(ProvLicenseDCAT())
    # PRESERVATION
    accrual = analyser.add(DatasetAccrualPeriodicityDCAT())
    format = analyser.add(DistributionFormatsDCAT())
    mediaType = analyser.add(DistributionMediaTypesDCAT())
    byteSize = analyser.add(DistributionByteSizeDCAT())
    # DATE
    issued = analyser.add(DatasetCreationDCAT())
    modified = analyser.add(DatasetModificationDCAT())
    distr_issued = analyser.add(DistributionIssuedDCAT())
    distr_modified = analyser.add(DistributionModifiedDCAT())
    # TEMPORAL
    temporal = analyser.add(DatasetTemporalDCAT())
    # SPATIAL
    spatial = analyser.add(DatasetSpatialDCAT())

    ds = dbm.getDatasets(snapshot=sn, portalID=p_id)
    d_iter = Dataset.iter(ds)
    process_all(analyser, d_iter)

    print ds_count.getResult()
    print temporal.getResult()
    print spatial.getResult()
    print accrual.getResult()
    print concact_point.getResult()
    print publisher.getResult()


if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    sn = 1542
    #general_stats(dbm, sn)
    exists(dbm, sn)
