'''
Created on Aug 14, 2015

@author: jumbrich
'''
import pandas as pd
from odpw.reporting.evolution_reporter import SystemEvolutionReporter

from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.evolution import DatasetEvolution, ResourceEvolution, \
    SystemEvolutionAnalyser, DatasetDCATMetricsEvolution, \
    PMDCountEvolution
from odpw.db.dbm import PostgressDBM
from odpw.db.models import PortalMetaData
from odpw.reporting.reporters import Report, SystemEvolutionReport
from odpw.reporting.reporters import Reporter, UIReporter, CLIReporter, \
    CSVReporter


class EvolutionReporter(Reporter, UIReporter, CLIReporter, CSVReporter):
    
    def __init__(self, analyser):
        super(EvolutionReporter, self).__init__(analyser)
        
    
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

class NVD3EvolutionReporter(EvolutionReporter):

    def __init__(self, analyser, key):
        print analyser
        super(NVD3EvolutionReporter, self).__init__(analyser)
        self.key=key

    def uireport(self):
        
        res={}
        for sn, dkv in  self.a.getResult().items():
            for k,v in dkv.items():
                if k not in res:
                    res[k]=[]
                d={'snapshot':sn, 'value':v}
                res[k].append(d)    
                
        return {self.key+'_evolv':res} 


    
class DatasetEvolutionReporter(EvolutionReporter):

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



def portalEvolution_report(dbm, sn, portal_id, metrics=None):
    
    aset = AnalyserSet()
    de=aset.add(DatasetEvolution())
    rese= aset.add(ResourceEvolution())
    
    ds = aset.add(PMDCountEvolution())
    
    ex = ['ExAc', 'ExDi', 'ExCo', 'ExRi', 'ExPr', 'ExDa', 'ExTe', 'ExSp']
    co = ['CoAc', 'CoCE', 'CoCU', 'CoDa', 'CoLi', 'CoFo']
    op = ['OpFo', 'OpMa', 'OpLi']
    re = ['ReRe', 'ReDa']
    ac = ['AcFo', 'AcSi']
    
    dcatEx = aset.add(DatasetDCATMetricsEvolution(ex))
    dcatCo = aset.add(DatasetDCATMetricsEvolution(co))
    dcatOp = aset.add(DatasetDCATMetricsEvolution(op))
    dcatRe = aset.add(DatasetDCATMetricsEvolution(re))
    dcatAc = aset.add(DatasetDCATMetricsEvolution(ac))

    
    it = dbm.getPortalMetaDatasUntil(snapshot=sn, portalID=portal_id)
    aset = process_all(aset, PortalMetaData.iter(it))
    
    rep = Report([
                    NVD3EvolutionReporter(de,"ds"),
                    NVD3EvolutionReporter(rese,"res"),
                    NVD3EvolutionReporter(dcatEx, 'ex'),
                    NVD3EvolutionReporter(dcatCo, 'co'),
                    NVD3EvolutionReporter(dcatOp, 'op'),
                    NVD3EvolutionReporter(dcatRe, 're'),
                    NVD3EvolutionReporter(dcatAc, 'ac'),
                    
                ])
   
    return rep



def dcat_evolution(dbm, from_sn, to_sn, portal_id, metrics):
    aset = AnalyserSet()
    ds = aset.add(PMDCountEvolution())
    dcat = aset.add(DatasetDCATMetricsEvolution(metrics))

    #re= aset.add(ResourceEvolution())

    it = dbm.getPortalMetaDatasUntil(snapshot=to_sn, from_sn=from_sn, portalID=portal_id)
    aset = process_all(aset, PortalMetaData.iter(it))

    rep = Report([
                    DatasetEvolutionReporter(dcat),
                    #ResourcesEvolutionReporter(re)

                ])
    return rep

def systemevolution(dbm):
    """
    
    """
    aset = AnalyserSet()
    sysev= aset.add(SystemEvolutionAnalyser())

    process_all(aset, dbm.systemEvolution())

    sysevre= SystemEvolutionReporter(sysev)
    
    rep = SystemEvolutionReport([sysevre])
    
    return rep
    
#===============================================================================
#     
#     with Timer(verbose=True) as t:
#         p={}
#         for P in Portal.iter(dbm.getPortals()):
#             p[P.id]=P.software
# 
#             de=aset.add(DatasetEvolution())
#             re= aset.add(ResourceEvolution())
#             se= aset.add(SystemSoftwareEvolution(p))
#             rae= aset.add(ResourceAnalysedEvolution())
#     
#             it = dbm.getPortalMetaDatas()
#             aset = process_all(aset, PortalMetaData.iter(it))
#     
#     print "---" 
#     print de.getResult()
#     
#     rep = SystemEvolutionReport([
#                                  DatasetEvolutionReporter(de),
#                                  ResourcesEvolutionReporter(re),
#                                  SystemSoftwareEvolutionReporter(se),
#                                  ResourceAnalyseReporter(rae)
#                                  ])
#    
#     return rep
#===============================================================================


import matplotlib.pyplot as plt
import matplotlib.colors as colors
import matplotlib.cm as cmx
import os


def scalar_plot(curves, curve_labels, curve_colors, xlabels, dir, filename):
    values = range(len(curves))

    fig = plt.figure(figsize=(8, 4.5))
    ax = fig.add_subplot(111)
    #ax.set_ylim([0, 1])

    jet = cm = plt.get_cmap('jet')
    cNorm = colors.Normalize(vmin=0, vmax=values[-1])
    scalarMap = cmx.ScalarMappable(norm=cNorm, cmap=jet)

    lines = []
    i = 0
    for c in curves:
        lines.append(ax.plot(c, color=curve_colors[i], label=curve_labels[i]))
        i += 1

    # added this to get the legend to work
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels, loc='lower right')
    x = range(0, len(xlabels))
    plt.xticks(x, xlabels, fontsize=10)

    print "Saving figure to ", os.path.join(dir, filename)
    plt.savefig(os.path.join(dir, filename), bbox_inches="tight")



if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    to_sn = 1550
    from_sn = 1535
    portal_id = 'data_hdx_rwlabs_org'
    #metrics = ['CoLi', 'CoFo']

    datasets= []
    resources= []

    ex = ['ExAc', 'ExDi', 'ExCo', 'ExRi', 'ExPr', 'ExDa', 'ExTe', 'ExSp']
    co = ['CoAc', 'CoCE', 'CoCU', 'CoDa', 'CoLi', 'CoFo']
    op = ['OpFo', 'OpMa', 'OpLi']
    re = ['ReRe', 'ReDa']
    metrics = ['AcFo', 'AcSi']


    rep = dcat_evolution(dbm, from_sn=from_sn, to_sn=to_sn, portal_id=portal_id, metrics=metrics)
    rep = rep.uireport()['datasetevolutionreporter']

    val = {m: [] for m in metrics}
    #coli = []
    #cofo = []
    xlabels = [str(x) for x in range(from_sn, to_sn)]

    for sn in range(from_sn, to_sn):
        for el in rep:
            if el['snapshot'] == sn:
                for k in metrics:
                    if el['key'] == k:
                        val[k].append(el['value'])
                if el['key'] == 'datasets':
                    datasets.append(el['value'])
                #else:
                #    datasets.append(-1)
                if el['key'] == 'resources':
                    resources.append(el['value'])
                    #else:
                    #    resources.append(-1)


    scalar_plot(val.values(), metrics, "bgrcmykw", xlabels, '.', 'accuracy.pdf')
    #scalar_plot([datasets], ['datasets'], ['blue'], xlabels, '.', 'datasets.pdf')
    #scalar_plot([resources], ['resources'], ['blue'], xlabels, '.', 'resources.pdf')