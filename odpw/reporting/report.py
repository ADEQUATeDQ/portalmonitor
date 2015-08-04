'''
Created on Jul 9, 2015

@author: jumbrich
'''
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.fetching import CKANTagsCount, CKANLicenseCount, CKANOrganizationsCount, CKANFormatCount
from odpw.analysers.socrata_analysers import SocrataTagsCount
from odpw.db.dbm import PostgressDBM
from odpw.db.models import Dataset
from odpw.reporting.reporters import ReporterEngine, dftopk, TagReporter, LicensesReporter

if __name__ == '__main__':
    
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)


    # portals = dbm.getPortals(software='Socrata')
    # for p in portals:
    #     a1 = AnalyserSet()
    #     ta = SocrataTagsCount()
    #     a1.add(ta)
    #     ds = dbm.getDatasets(portalID=p.id, snapshot=1531)
    #     process_all(a1, Dataset.iter(ds))
    #     tags.append(ta)

    portals = dbm.getPortals(software='CKAN')

    tags = []
    licenses = []

    i = 0
    for p in portals:
        a1 = AnalyserSet()
        ta = CKANTagsCount()
        a1.add(ta)
        oa = CKANOrganizationsCount()
        a1.add(oa)
        fa = CKANFormatCount()
        a1.add(fa)
        la = CKANLicenseCount()
        a1.add(la)

        ds = dbm.getDatasets(portalID=p.id, snapshot=1531)
        process_all(a1, Dataset.iter(ds))

        tags.append(ta)
        licenses.append(la)
        if i == 2:
            break
        i += 1

    ta = AnalyserSet([CKANTagsCount()])
    process_all(ta, tags)

    la = AnalyserSet([CKANLicenseCount()])
    process_all(la, licenses)

    tags_rep = TagReporter(ta)
    l_rep = LicensesReporter(la)

    re = ReporterEngine([tags_rep, l_rep])
    re.csvreport('tmp')

    tmp = dftopk(tags_rep.getDataFrame(), 'Count', k=50)
    print tmp

