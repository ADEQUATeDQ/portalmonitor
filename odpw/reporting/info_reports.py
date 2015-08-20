'''
Created on Aug 14, 2015

@author: jumbrich
'''
from odpw.analysers import process_all, AnalyserSet
from odpw.analysers.core import DBAnalyser
from odpw.reporting.reporters import SoftWareDistReporter, ISO3DistReporter,\
    PortalListReporter, SnapshotsPerPortalReporter, DatasetSumReporter,\
    ResourceCountReporter, ResourceSizeReporter, TagReporter,\
    OrganisationReporter, FormatCountReporter, Report

from odpw.analysers.count_analysers import DCATTagsCount, DCATOrganizationsCount,\
    DCATFormatCount, PMDResourceStatsCount, DatasetCount
from odpw.analysers.resource_analysers import ResourceSize

def systeminfo(dbm):
    """
        country and software distribution of portals in the system
        full list of portals
    """
    
    a = process_all(DBAnalyser(),dbm.getSoftwareDist())
    ab = process_all(DBAnalyser(),dbm.getCountryDist())
    pa = process_all(DBAnalyser(),dbm.getPortals())
    return  Report([SoftWareDistReporter(a),
                 ISO3DistReporter(ab),
                 PortalListReporter(pa)
                 ])


def portalinfo(dbm, sn, portal_id):
    a= process_all( DBAnalyser(), dbm.getSnapshots( portalID=portal_id,apiurl=None))
    r=SnapshotsPerPortalReporter(a,portal_id)

        
    aset = AnalyserSet()
    #lc=aset.add(CKANLicenseCount())# how many licenses
    #lcc=aset.add(CKANLicenseConformance())

    tc= aset.add(DCATTagsCount())   # how many tags
    oc= aset.add(DCATOrganizationsCount())# how many organisations
    fc= aset.add(DCATFormatCount())# how many formats

    resC= aset.add(PMDResourceStatsCount())   # how many resources
    dsC=dc= aset.add(DatasetCount())    # how many datasets
    rsize=aset.add(ResourceSize())

    #use the latest portal meta data object
    pmd = dbm.getPortalMetaData(portalID=portal_id, snapshot=sn)
    aset = process_all(aset, [pmd])

    rep = Report([r,
                    DatasetSumReporter(dsC),
    ResourceCountReporter(resC),
    ResourceSizeReporter(rsize),
    #LicensesReporter(lc,lcc,topK=3),
    TagReporter(tc,dc, topK=3),
    OrganisationReporter(oc, topK=3),
    FormatCountReporter(fc, topK=3)])
    
    return rep