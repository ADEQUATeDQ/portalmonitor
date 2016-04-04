
from odpw.analysers.evolution import DatasetEvolution, ResourceEvolution, EvolutionAggregator, QualityEvolution
from odpw.analysers.organisation_analyser import OrganisationAggregator, SoftwareAggregator, PerOrganisationAnalyser, \
    ISOAggregator, PortalAggregator
from odpw.analysers.fetching import DCATResourceInDSAge, DCATDatasetAge
from odpw.analysers.quality.analysers import DCATDMD
from odpw.analysers.statuscodes import DatasetStatusCode
from odpw.analysers.count_analysers import DCATLicenseCount, DCATTagsCount, DCATOrganizationsCount, DCATFormatCount, \
    PMDResourceStatsCount, DatasetCount, DCATDistributionCount, SnapshotCount
from odpw.analysers.process_period_analysers import FetchPeriod, HeadPeriod
from odpw.analysers import process_all, AnalyserSet, SAFEAnalyserSet
from odpw.analysers.core import DBAnalyser, DCATConverter
from odpw.reporting.reporters.portal_reporter import PortalBasicReport, PortalReporter

from odpw.utils.util import ErrorHandler as eh, getPreviousWeek

from odpw.db.models import Dataset, PortalMetaData

import structlog
log =structlog.get_logger()


def analyse_organisations(Portal, sn, analysers):
    ae = SAFEAnalyserSet()

    a=[ DCATLicenseCount
        ,DCATTagsCount
        ,DCATOrganizationsCount
        ,DCATFormatCount
        ,DCATResourceInDSAge
        ,DCATDatasetAge
        ,DatasetCount
        ,DCATDistributionCount
        ,DatasetStatusCode
        ,DCATDMD
    ]
    oa=ae.add(OrganisationAggregator(Portal, sn, a))

    process_all( ae, [analysers])

    return oa


def analyse_perOrganisation(dbm, sn, P):
    """ Anaylse the datasets and group them by organisation
    :param dbm:
    :param sn:
    :param P:
    :return:
    """
    prev_sn=getPreviousWeek(sn)
    datasetsfrom={ D.id:D for D in Dataset.iter(dbm.getDatasets(portalID=P.id, snapshot=prev_sn))}

    #pmd= dbm.getPortalMetaData(portalID=P.id, snapshot=sn)
    #fp= FetchPeriod()
    #fp.analyse_PortalMetaData(pmd)


    ae = SAFEAnalyserSet()

    ae.add(DCATConverter(P))
    a=[ DCATLicenseCount
        ,DCATTagsCount
        ,DCATOrganizationsCount
        ,DCATFormatCount
        ,DCATResourceInDSAge
        ,DCATDatasetAge
        ,DatasetCount
        ,DCATDistributionCount
        ,DatasetStatusCode
        ,DCATDMD
    ]
    oa=ae.add(PerOrganisationAnalyser(P, sn, a, datasetsfrom))

    iter = Dataset.iter(dbm.getDatasets(portalID=P.id, snapshot=sn))
    process_all( ae, iter)



    return oa


def analyse_portalAll(dbm, sn, P):

    prev_sn=getPreviousWeek(sn)
    datasetsfrom={ D.id:D for D in Dataset.iter(dbm.getDatasets(portalID=P.id, snapshot=prev_sn))}

    ae = SAFEAnalyserSet()

    ae.add(DCATConverter(P))
    a=[DCATLicenseCount
     ,DCATTagsCount
     ,DCATOrganizationsCount
     ,DCATFormatCount
     ,DCATResourceInDSAge
     ,DCATDatasetAge
     ,DatasetCount
     ,DCATDistributionCount
     ,DatasetStatusCode
     ,DCATDMD
     ]
    oa=ae.add(OrganisationAggregator(P, sn, a, datasetsfrom))

    iter = Dataset.iter(dbm.getDatasets(portalID=P.id, snapshot=sn))
    process_all( ae, iter)

    return oa

def aggregate_allportals(portalanalysers):
    ae = SAFEAnalyserSet()

    a=[DCATLicenseCount
     ,DCATTagsCount
     ,DCATOrganizationsCount
     ,DCATFormatCount
     ,DCATResourceInDSAge
     ,DCATDatasetAge
     ,DatasetCount
     ,DCATDistributionCount
     ,DatasetStatusCode
     ,DCATDMD
     ,FetchPeriod
     ]
    oa=ae.add(PortalAggregator(a))

    iter = portalanalysers
    process_all( ae, iter)

    return oa


def aggregate_perISO(orgaagganalysers):
    ae = SAFEAnalyserSet()

    a=[DCATLicenseCount
     ,DCATTagsCount
     ,DCATOrganizationsCount
     ,DCATFormatCount
     ,DCATResourceInDSAge
     ,DCATDatasetAge
     ,DatasetCount
     ,DCATDistributionCount
     ,DatasetStatusCode
     ,DCATDMD
     ,FetchPeriod
     ]
    oa=ae.add(ISOAggregator(a))

    iter = orgaagganalysers
    process_all( ae, iter)

    return oa

def aggregate_perSoftwareISO(orgaagganalysers):
    ae = SAFEAnalyserSet()

    a=[DCATLicenseCount
     ,DCATTagsCount
     ,DCATOrganizationsCount
     ,DCATFormatCount
     ,DCATResourceInDSAge
     ,DCATDatasetAge
     ,DatasetCount
     ,DCATDistributionCount
     ,DatasetStatusCode
     ,DCATDMD
     ,FetchPeriod
     ]
    oa=ae.add(SoftwareAggregator(a))

    iter = orgaagganalysers
    process_all( ae, iter)

    return oa



def report_portalAll(dbm, sn, portal_id):
    """ This is a dataset iteration version
    :param dbm:
    :param sn:
    :param portal_id:
    :return:
    """
    P = dbm.getPortal(portalID=portal_id)

    try:
        ## get the pmd for this job
        pmd = dbm.getPortalMetaData(portalID=P.id, snapshot=sn)
        if not pmd:
            pmd = PortalMetaData(portalID=P.id, snapshot=sn)
            dbm.insertPortalMetaData(pmd)
        else:
            print sn, pmd.qa_stats
        pmd.qa_stats={}

        prev_sn=getPreviousWeek(sn)
        iter = Dataset.iter(dbm.getDatasets(portalID=P.id, snapshot=prev_sn))

        datasetsfrom={}
        for D in iter:
            datasetsfrom[D.id]=D

        ae = SAFEAnalyserSet()

        ae.add(DCATConverter(P))
        a=[DCATLicenseCount
         ,DCATTagsCount
         ,DCATOrganizationsCount
         ,DCATFormatCount
         ,DCATResourceInDSAge
         ,DCATDatasetAge
         ,DatasetCount
         ,DCATDistributionCount
         ,DatasetStatusCode
         ,DCATDMD
         ]
        oa=ae.add(OrganisationAggregator(a, datasetsfrom))

        iter = Dataset.iter(dbm.getDatasets(portalID=P.id, snapshot=sn))
        process_all( ae, iter)



        ae.update(pmd)

        print sn, pmd.datasets
        dbm.updatePortalMetaData(pmd)


        ##### PMDS -> and evolution #####
        ae = SAFEAnalyserSet()
        dsevolv= ae.add(DatasetEvolution())
        resevolv= ae.add(ResourceEvolution())
        qaevolv= ae.add(QualityEvolution())

        iter= PortalMetaData.iter(dbm.getPortalMetaDatasUntil( portalID=portal_id, snapshot=sn))
        process_all(ae,iter)

        ae = SAFEAnalyserSet()
        snapshots= ae.add(SnapshotCount())
        iter= PortalMetaData.iter(dbm.getPortalMetaDatas( portalID=portal_id))
        process_all(ae,iter)

        ea= EvolutionAggregator()
        process_all(ea, [dsevolv,resevolv,qaevolv])

        fetch_period= FetchPeriod()
        process_all(fetch_period,[pmd])

        return PortalReporter(P,sn, [oa,snapshots,fetch_period,ea])


    except Exception as exc:
        eh.handleError(log, "PortalFetch", exception=exc, pid=P.id, snapshot=sn, exc_info=True)

def report_portalbasics(dbm, sn, portal_id):
    """
    :param dbm:
    :param sn:
    :param portal_id:
    :return: a PortalBasicReport containing
        #snapshots
        iso
        system
        id
        url
        apiurl
    """

    P = dbm.getPortal(portalID=portal_id)
    a = process_all( DBAnalyser(), dbm.getSnapshotsFromPMD( portalID=portal_id))

    return PortalBasicReport(a, P)



def report_portalSnapshotInfo(dbm, sn, portal_id):
    aset = AnalyserSet()
    lc=aset.add(DCATLicenseCount())# how many licenses
    tc= aset.add(DCATTagsCount())   # how many tags
    oc= aset.add(DCATOrganizationsCount())# how many organisations
    fc= aset.add(DCATFormatCount())# how many formats

    resC= aset.add(PMDResourceStatsCount())   # how many resources
    dsC=dc= aset.add(DatasetCount())    # how many datasets




    fa= aset.add(FetchPeriod())
    ha= aset.add(HeadPeriod())

    pmd = dbm.getPortalMetaData(portalID=portal_id, snapshot=sn)

    aset = process_all(aset, [pmd])

    rep = Report([r,
                    DatasetSumReporter(dsC),
                    ResourceCountReporter(resC),
                    ResourceSizeReporter(rsize),
                    LicenseCountReporter(lc,distinct=True),
                    TagReporter(tc,dc, distinct=True),
                    OrganisationReporter(oc, distinct=True),
                    FormatCountReporter(fc,  distinct=True),

                    FetchTimePeriodReporter(fa),
                    HeadTimePeriodReporter(ha)
                ]
                )

    return rep