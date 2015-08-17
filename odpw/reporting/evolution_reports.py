'''
Created on Aug 14, 2015

@author: jumbrich
'''
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.evolution import DatasetEvolution, ResourceEvolution
from odpw.db.models import PortalMetaData
from odpw.reporting.reporters import Report
from odpw.reporting.evolution_reporter import DatasetEvolutionReporter,\
    ResourcesEvolutionReporter


def portalevolution(dbm, sn, portal_id):
    print portal_id, sn
    aset = AnalyserSet()
    de=aset.add(DatasetEvolution())
    re= aset.add(ResourceEvolution())
    
    it = dbm.getPortalMetaDatasUntil(snapshot=sn, portalID=portal_id)
    aset = process_all(aset, PortalMetaData.iter(it))
    
    rep = Report([
                    DatasetEvolutionReporter(de),
                    ResourcesEvolutionReporter(re),
                ])
   
    return rep
    



def systemevolution(dbm):
    """
    
    """
    aset = AnalyserSet()
    
    p={}
    for P in Portal.iter(dbm.getPortals()):
        p[P.id]=P.software

    de=aset.add(DatasetEvolution())
    re= aset.add(ResourceEvolution())
    se= aset.add(SystemSoftwareEvolution(p))
    
    it = dbm.getPortalMetaDatas()
    aset = process_all(aset, PortalMetaData.iter(it))
    
    rep = SystemEvolutionReport([
                                 DatasetEvolutionReporter(de),
                                 ResourcesEvolutionReporter(re),
                                 SystemSoftwareEvolutionReporter(se)
                                 ])
   
    return rep
    