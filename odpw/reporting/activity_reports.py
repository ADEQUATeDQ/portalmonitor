'''
Created on Aug 14, 2015

@author: jumbrich
'''
from odpw.analysers import process_all, AnalyserSet
from odpw.analysers.pmd_analysers import PMDActivityAnalyser
from odpw.analysers.process_period_analysers import HeadPeriod, FetchPeriod, \
    FetchProcessAnalyser
from odpw.db.models import PortalMetaData
from odpw.reporting.time_period_reporting import FetchTimePeriodReporter,\
    HeadTimePeriodReporter, FetchProcessReporter
from odpw.utils.timer import Timer
from reporting.reporters.reporters import SystemActivityReporter, Report


def systemfetchactivity(dbm, sn):
    it =  dbm.dictiter(dbm.getPortalMetaDatas(snapshot=sn, selectVars=[dbm.pmd.c.portal_id,dbm.pmd.c.fetch_stats]))
    aset = AnalyserSet()
    pmda=aset.add(PMDActivityAnalyser(sn))
    fa= aset.add(FetchPeriod())
    
    aset = process_all(aset,it)
    rep = Report([SystemActivityReporter(pmda,snapshot=sn, dbds=None, dbres= None, dbresproc=None),
                  FetchTimePeriodReporter(fa)])
    
    return rep
    
    
    
def systemactivity(dbm, sn):
    
    with Timer(verbose=True, key="IterPMDs") as t:
        it =PortalMetaData.iter(dbm.getPortalMetaDatas(snapshot=sn))
        aset = AnalyserSet()
        
        pmda=aset.add(PMDActivityAnalyser(sn))
        fa= aset.add(FetchPeriod())
        ha= aset.add(HeadPeriod())
        
        #ftsa= aset.add(FetchTimeSpanAnalyser())
        #fpa= aset.add(FetchProcessAnalyser())
        #htsa= aset.add(HeadTimeSpanAnalyser())
        aset = process_all(aset,it)
    
    with Timer(verbose=True, key="CountDS") as t:
        totalDS = dbm.countDatasets(snapshot=sn)
    
    with Timer(verbose=True, key="CountRes") as t:
        totalRes= dbm.countResourcesPerSnapshot(snapshot=sn)
    
    with Timer(verbose=True, key="CountProcessedRes") as t:
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