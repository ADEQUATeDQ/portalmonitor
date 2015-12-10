

from odpw.analysers.quality.new.existence_dcat import *
from odpw.analysers.quality.new.conformance_dcat import *
from odpw.analysers.quality.new.open_dcat_format import IANAFormatDCATAnalyser,\
    FormatOpennessDCATAnalyser, FormatMachineReadableDCATAnalyser
from odpw.analysers.quality.new.open_dcat_license import LicenseOpennessDCATAnalyser
from odpw.analysers.quality.new.retrievability import DSRetrieveMetric
from odpw.analysers.statuscodes import DatasetStatusCode


def dcat_analyser():
    dcat_analyser = []
    ##################### EXISTS ######################################
    # ACCESS
    dcat_analyser.append(AnyMetric([AccessUrlDCAT(), DownloadUrlDCAT()], id='ExAc'))
    # DISCOVERY
    dcat_analyser.append(AverageMetric([DatasetTitleDCAT(), DatasetDescriptionDCAT(), DatasetKeywordsDCAT(), DistributionTitleDCAT(), DistributionDescriptionDCAT()], id='ExDi'))
    # CONTACT
    dcat_analyser.append(AnyMetric([DatasetContactDCAT(), DatasetPublisherDCAT()], id='ExCo'))
    # LICENSE
    dcat_analyser.append(ProvLicenseDCAT())
    # PRESERVATION
    dcat_analyser.append(AverageMetric([DatasetAccrualPeriodicityDCAT(), DistributionFormatsDCAT(), DistributionMediaTypesDCAT(), DistributionByteSizeDCAT()], id='ExPr'))
    # DATE
    dcat_analyser.append(AverageMetric([DatasetCreationDCAT(), DatasetModificationDCAT(), DistributionIssuedDCAT(), DistributionModifiedDCAT()], id='ExDa'))
    # TEMPORAL
    dcat_analyser.append(DatasetTemporalDCAT())
    # SPATIAL
    dcat_analyser.append(DatasetSpatialDCAT())

    ####################### CONFORMANCE ###########################
    # ACCESS
    dcat_analyser.append(AnyConformMetric([ConformAccessUrlDCAT(), ConformDownloadUrlDCAT()], id='CoAc'))
    # CONTACT
    dcat_analyser.append(AnyConformMetric([EmailConformContactPoint(), EmailConformPublisher()], id='CoCE'))
    dcat_analyser.append(AnyConformMetric([UrlConformContactPoint(), UrlConformPublisher()], id='CoCU'))

    # DATE
    dcat_analyser.append(AverageConformMetric([DateConform(dcat_access.getCreationDate),
                                             DateConform(dcat_access.getModificationDate),
                                             DateConform(dcat_access.getDistributionCreationDates),
                                             DateConform(dcat_access.getDistributionModificationDates)], id='CoDa'))
    # LICENSE
    dcat_analyser.append(LicenseConform())
    # FORMAT
    dcat_analyser.append(IANAFormatDCATAnalyser())

    ####################### OPENNESS ###########################
    dcat_analyser.append(FormatOpennessDCATAnalyser())
    dcat_analyser.append(FormatMachineReadableDCATAnalyser())
    dcat_analyser.append(LicenseOpennessDCATAnalyser())

    ####################### RETRIEVABILITY ###########################
    status_code = DatasetStatusCode()
    dcat_analyser.append(status_code)
    dcat_analyser.append(DSRetrieveMetric(status_code))

    return dcat_analyser
