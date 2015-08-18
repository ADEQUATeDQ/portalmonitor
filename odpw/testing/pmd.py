'''
Created on Aug 6, 2015

@author: jumbrich
'''
from odpw.analysers.core import ElementCountAnalyser
from odpw.analysers.count_analysers import DatasetCount
from odpw.analysers.pmd_analysers import PMDDatasetCountAnalyser
from odpw.db.dbm import PostgressDBM
from odpw.analysers import AnalyserSet, process_all
from odpw.db.models import Dataset, PortalMetaData, Portal
from odpw.reporting.reporters import ElementCountReporter, Report

if __name__ == '__main__':
    
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)

    #id='data_wu_ac_at'
    sn=1533
    pmd = dbm.getPortalMetaDatas(snapshot=sn)
    #portals = dbm.getPortals(software='CKAN')

    #iter = Portal.iter(portals)
    iter = PortalMetaData.iter(pmd)

    ae = AnalyserSet()
    #ca=ae.add(ElementCountAnalyser(funct=lambda portal: portal.iso3))
    bins = [0,50,100,500,1000,5000,10000,50000,100000,1000000]
    ds_histogram = ae.add(PMDDatasetCountAnalyser(bins=bins))

    process_all(ae, iter)

    #re = ElementCountReporter(ca, ['Country', 'Count'], topK=20)
    print 'ds_histogram', ds_histogram.getResult()
    #engine = Report([re])
    #engine.csvreport('tmp')

