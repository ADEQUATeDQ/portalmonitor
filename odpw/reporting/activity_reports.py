'''
Created on Aug 14, 2015

@author: jumbrich
'''
from odpw.db.models import PortalMetaData
from odpw.analysers import process_all, AnalyserSet
from odpw.analysers.pmd_analysers import PMDActivityAnalyser

from odpw.reporting.reporters import SystemActivityReporter, Report
from odpw.analysers.process_period_analysers import HeadPeriod, FetchPeriod
from odpw.reporting.time_period_reporting import FetchTimePeriodReporter,\
    HeadTimePeriodReporter

def systemactivity(dbm, sn):
    print 'starting'
    it =PortalMetaData.iter(dbm.getPortalMetaDatas(snapshot=sn))
    aset = AnalyserSet()
    
    pmda=aset.add(PMDActivityAnalyser())
    
    fa= aset.add(FetchPeriod())
    ha= aset.add(HeadPeriod())
    aset = process_all(aset,it)
    
    print "processed"
    print "count ds"
    totalDS = dbm.countDatasets(snapshot=sn)
    print totalDS
    print "count res total"
    totalRes= dbm.countResourcesPerSnapshot(snapshot=sn)
    print totalRes
    processedRes= dbm.countProcessedResourcesPerSnapshot(snapshot=sn)
    
    return Report([SystemActivityReporter(pmda,snapshot=sn, dbds=totalDS, dbres= totalRes, dbresproc=processedRes),
                   FetchTimePeriodReporter(fa),
                   HeadTimePeriodReporter(ha)])
    
    
    