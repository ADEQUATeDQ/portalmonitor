'''
Created on Aug 14, 2015

@author: jumbrich
'''
from odpw.db.models import PortalMetaData
from odpw.analysers import process_all
from odpw.analysers.pmd_analysers import PMDActivityAnalyser

from odpw.reporting.reporters import SystemActivityReporter, Report

def systemactivity(dbm, sn):
    print 'Start iteration'
    it =PortalMetaData.iter(dbm.getPortalMetaDatas(snapshot=sn, portalID=None))
    a = process_all(PMDActivityAnalyser(),it)
    print 'done iteration'
    totalDS = dbm.countDatasets(snapshot=sn)
    totalRes= dbm.countResources(snapshot=sn)

    return Report([SystemActivityReporter(a,snapshot=sn, dbds=totalDS, dbres= totalRes)])
    