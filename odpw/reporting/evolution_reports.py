'''
Created on Aug 14, 2015

@author: jumbrich
'''
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.evolution import DatasetEvolution, ResourceEvolution,\
    ResourceAnalysedEvolution, SystemSoftwareEvolution
from odpw.db.models import PortalMetaData, Portal
from odpw.reporting.reporters import Report, SystemEvolutionReport

from odpw.reporting.reporters import Reporter, UIReporter, CLIReporter,\
    DFtoListDict, CSVReporter
import pandas as pd



class EvolutionReporter(Reporter, UIReporter, CLIReporter, CSVReporter):
    
    def __init__(self, analyser):
        super(EvolutionReporter, self).__init__()
        self.a=analyser
    
    def getDataFrame(self):
        if  self.df is None:
            res=[]
            for sn, dkv in  self.a.getResult().items():
                d={'snapshot':sn}
                for k,v in dkv.items():
                    d[k]=v
                res.append(d) 
            self.df= pd.DataFrame(res)
        return self.df
    
class DatasetEvolutionReporter(EvolutionReporter):
    pass

    def uireport(self):
        res=[]
        for sn, dkv in  self.a.getResult().items():
            for k,v in dkv.items():
                d={'snapshot':sn, 'value':v, 'key':k}
                res.append(d)
        return {self.name():res} 
            
class ResourcesEvolutionReporter(EvolutionReporter):
    pass
class ResourceAnalyseReporter(EvolutionReporter):
    pass 
class SystemSoftwareEvolutionReporter(EvolutionReporter):
    pass
    
    

def portalevolution(dbm, sn, portal_id):
    
    aset = AnalyserSet()
    de=aset.add(DatasetEvolution())
    re= aset.add(ResourceEvolution())
    
    
    it = dbm.getPortalMetaDatasUntil(snapshot=sn, portalID=portal_id)
    aset = process_all(aset, PortalMetaData.iter(it))
    
    rep = Report([
                    DatasetEvolutionReporter(de),
                    ResourcesEvolutionReporter(re)
                    
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
    rae= aset.add(ResourceAnalysedEvolution())
    
    it = dbm.getPortalMetaDatas()
    aset = process_all(aset, PortalMetaData.iter(it))
    
    rep = SystemEvolutionReport([
                                 DatasetEvolutionReporter(de),
                                 ResourcesEvolutionReporter(re),
                                 SystemSoftwareEvolutionReporter(se),
                                 ResourceAnalyseReporter(rae)
                                 ])
   
    return rep
