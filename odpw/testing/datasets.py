'''
Created on Aug 10, 2015

@author: jumbrich
'''
from odpw.analysers.quality.new.open_dcat_format import FormatOpennessDCATAnalyser, FormatMachineReadableDCATAnalyser
from odpw.analysers.quality.new.open_dcat_license import LicenseOpennessDCATAnalyser
from odpw.db.dbm import PostgressDBM
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.core import DCATConverter
from odpw.db.models import Dataset
from odpw.utils.dataset_converter import dict_to_dcat
from odpw.analysers.dbm_handlers import DCATDistributionInserter
from odpw.analysers.count_analysers import DCATDistributionCount, DatasetCount

if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)



    #"{"www_data_gc_ca": ["fe3a1013-9877-40ac-8d90-61a78abcdc75"]}"
    id="data_gv_at"
    #did="fe3a1013-9877-40ac-8d90-61a78abcdc75"
    sn=1533

    d = dbm.getDatasets(snapshot=sn, portalID=id)


    a_set = AnalyserSet()

    dc = a_set.add(DCATConverter(dbm.getPortal(portalID=id)))
    d4 = a_set.add(DatasetCount())
    d1 = a_set.add(DCATDistributionCount())
    d2 = a_set.add(FormatOpennessDCATAnalyser())
    d3 = a_set.add(FormatMachineReadableDCATAnalyser())
    d5 = a_set.add(LicenseOpennessDCATAnalyser())

    process_all(a_set, Dataset.iter(d))

    print 'datasets', d4.getResult()
    print 'distributions', d1.getResult()
    print 'open format distributions', d2.getResult()
    print 'machine-readable format distributions', d3.getResult()
    print 'open license distributions', d5.getResult()

        