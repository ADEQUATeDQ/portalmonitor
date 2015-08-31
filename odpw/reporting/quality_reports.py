'''
Created on Aug 27, 2015

@author: jumbrich
'''
from odpw.analysers import AnalyserSet, process_all
from odpw.db.models import PortalMetaData
from odpw.reporting.reporters import Report
from odpw.analysers.statuscodes import DatasetStatusCode, ResourceStatusCode
from odpw.reporting.statuscode_reporter import DatasetStatusCodeReporter,\
    ResourcesStatusCodeReporter
from odpw.analysers.quality.new.retrievability import ResourceRetrievability,\
    DatasetRetrievability
from odpw.reporting.quality_reporters import DatasetRetrievabilityReporter,\
    ResourceRetrievabilityReporter


def portalquality(dbm, sn , portal_id):
    
    aset = AnalyserSet()
    
    dr=DatasetStatusCode()
    rr=ResourceStatusCode()
    
    drq=aset.add(DatasetRetrievability(dr))
    rrq=aset.add(ResourceRetrievability(rr))
    
    
    it = dbm.getPortalMetaDatas(snapshot=sn, portalID=portal_id)
    aset = process_all(aset, PortalMetaData.iter(it))
    
    
    rep = Report([
                    DatasetStatusCodeReporter(dr),
                    ResourcesStatusCodeReporter(rr),
                    DatasetRetrievabilityReporter(drq),
                    ResourceRetrievabilityReporter(rrq),
                    
                ])
   
    return rep