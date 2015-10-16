'''
Created on Aug 14, 2015

@author: jumbrich
'''
from odpw.db.models import PortalMetaData
from odpw.analysers import process_all, AnalyserSet
from odpw.analysers.pmd_analysers import PMDActivityAnalyser

from odpw.reporting.reporters import SystemActivityReporter, Report
from odpw.analysers.process_period_analysers import HeadPeriod, FetchPeriod,\
    FetchTimeSpanAnalyser, HeadTimeSpanAnalyser, FetchProcessAnalyser
from odpw.reporting.time_period_reporting import FetchTimePeriodReporter,\
    HeadTimePeriodReporter, FetchProcessReporter
from odpw.utils.timer import Timer

def systemactivity(dbm, sn):
    print 'starting'
    it =PortalMetaData.iter(dbm.getPortalMetaDatas(snapshot=sn))
    aset = AnalyserSet()
    
    pmda=aset.add(PMDActivityAnalyser(sn))
    
    fa= aset.add(FetchPeriod())
    ha= aset.add(HeadPeriod())
    #ftsa= aset.add(FetchTimeSpanAnalyser())
    #fpa= aset.add(FetchProcessAnalyser())
    #htsa= aset.add(HeadTimeSpanAnalyser())
    aset = process_all(aset,it)
    
    
    
    print "processed"
    print "count ds"
    with Timer(verbose=True, key="CountDS") as t:
        totalDS = dbm.countDatasets(snapshot=sn)
    print totalDS
    
    print "count res total"
    totalRes= dbm.countResourcesPerSnapshot(snapshot=sn)
    print totalRes
    
    processedRes= dbm.countProcessedResourcesPerSnapshot(snapshot=sn)
    
    return Report([SystemActivityReporter(pmda,snapshot=sn, dbds=totalDS, dbres= totalRes, dbresproc=processedRes),
                   FetchTimePeriodReporter(fa),
                   HeadTimePeriodReporter(ha)])
    
    
def fetch_process(dbm, snapshots):
    
    an = []
    hn=[]
    for sn in snapshots:
        it =PortalMetaData.iter(dbm.getPortalMetaDatas(snapshot=sn))
        
        aset = AnalyserSet()
        
        #aa.append( aset.add(PMDActivityAnalyser(sn) ) )
        an.append( aset.add(FetchProcessAnalyser(sn)) )
        
        
        process_all(aset, it)
    
    
    return Report([FetchProcessReporter(an)])