'''
Created on Aug 6, 2015

@author: jumbrich
'''
from odpw.db.dbm import PostgressDBM
from odpw.analysers import AnalyseEngine, AnalyserSet, process_all
from odpw.analysers.fetching import DatasetFetchUpdater, CKANLicenseCount,\
    CKANLicenseConformance, CKANOrganizationsCount
from odpw.db.models import Dataset
from odpw.utils.util import progressIterator
from odpw.reporting.reporters import LicensesReporter


if __name__ == '__main__':
    
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)

    id='data_wu_ac_at'
    sn=1532
    pmd = dbm.getPortalMetaData(portalID=id, snapshot=sn)

    ae = AnalyserSet()
    lc=ae.add(CKANLicenseCount())
    lcc=ae.add(CKANLicenseConformance())
    oc=ae.add(CKANOrganizationsCount())
    #ae.add(DatasetFetchUpdater(dbm))
    
    
    
    iter = Dataset.iter(dbm.getDatasets(portalID=id, snapshot=sn))
    process_all(ae,iter)
    
    ae.update(pmd)
    
    dbm.updatePortalMetaData(pmd)
    
    print oc.getResult()
    
    
    l_rep = LicensesReporter(lc,lcc)
    
    print l_rep.getDataFrame()
    
    ae = AnalyserSet()
    lc=ae.add(CKANLicenseCount())
    lcc=ae.add(CKANLicenseConformance())
    ae.analyse(pmd)
    l_rep = LicensesReporter(lc,lcc)
    print l_rep.getDataFrame()