'''
Created on Jul 9, 2015

@author: jumbrich
'''
import numpy as np

from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.fetching import CKANTagsCount, CKANLicenseCount, CKANOrganizationsCount, CKANFormatCount
from odpw.analysers.pmd_analysers import CompletenessHistogram, ContactabilityHistogram
from odpw.analysers.quality.analysers.completeness import CompletenessAnalyser
from odpw.analysers.quality.analysers.contactability import ContactabilityAnalyser
from odpw.db.dbm import PostgressDBM
from odpw.db.models import Dataset, PortalMetaData
from reporting.reporters.reporters import Report, dftopk, TagReporter, LicensesReporter

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

    pmds = dbm.getPortalMetaDatas(snapshot=1531)
    a = AnalyserSet()
    ca = CompletenessHistogram(bins=np.arange(0, 1.1, 0.1))
    conh = ContactabilityHistogram(bins=np.arange(0, 1.1, 0.1))
    a.add(ca)
    a.add(conh)
    process_all(a, PortalMetaData.iter(pmds))
    tmp = ca.getResult()
    print tmp
    tmp = conh.getResult()
    print tmp

    ds = dbm.getDatasets(portalID='data_wu_ac_at', snapshot=1531)
    a = AnalyserSet()
    compa = CompletenessAnalyser()
    conta = ContactabilityAnalyser()
    a.add(compa)
    a.add(conta)
    comh = CompletenessHistogram(bins=np.arange(0, 1.1, 0.1))
    conh = ContactabilityHistogram(bins=np.arange(0, 1.1, 0.1))
    a.add(comh)
    a.add(conh)
    process_all(a, Dataset.iter(ds))
    tmp = comh.getResult()
    print tmp
    tmp = conh.getResult()
    print tmp

    #hr = HistogramReporter(a)
    #re = Report([hr])
    #hr.getDataFrame()
    #re.csvreport('tmp')

    exit()

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

    re = Report([tags_rep, l_rep])
    re.csvreport('tmp')

    tmp = dftopk(tags_rep.getDataFrame(), 'Count', k=50)
    print tmp

