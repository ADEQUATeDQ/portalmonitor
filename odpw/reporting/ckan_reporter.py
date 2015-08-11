from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.core import ElementCountAnalyser, HistogramAnalyser, StatusCodeAnalyser
from odpw.analysers.count_analysers import DatasetCount, ResourceCount, CKANFormatCount, CKANTagsCount, CKANKeysCount, \
    CKANLicenseIDCount
from odpw.analysers.fetching import CKANKeyAnalyser, UsageAnalyser
from odpw.analysers.pmd_analysers import PMDDatasetCountAnalyser, PMDResourceCountAnalyser, CompletenessHistogram
from odpw.analysers.statuscodes import DatasetStatusCount
from odpw.db.dbm import PostgressDBM
from odpw.db.models import Dataset, PortalMetaData

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



if __name__ == '__main__':

    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)

    sn = 1532

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

    lid_count = pmd_analyser.add(CKANLicenseIDCount())

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
    compl_histogram = pmd_analyser.add(CompletenessHistogram)



    #np.arange(0,1,0.1)
    #pmds = dbm.getPortalMetaDatas(snapshot=sn)
    #pmd_iter = PortalMetaData.iter(pmds)

    #for p in portals:
    #    datasets = dbm.getDatasets(portalID=p.id, snapshot=sn)
    #    ds_iter = Dataset.iter(datasets)

    #process_all(aset, iter)

