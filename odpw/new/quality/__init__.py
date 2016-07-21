from abc import abstractmethod

from odpw.new.quality.conformance_dcat import *
from odpw.new.quality.existence_dcat import *
from odpw.new.quality.open_dcat_format import *
from odpw.new.quality.open_dcat_license import LicenseOpennessDCATAnalyser
from odpw.utils.util import ErrorHandler

import structlog
log = structlog.get_logger()


class Analyser(object):

    @classmethod
    def name(cls): return cls.__name__

    def analyse(self, node, *args, **kwargs):
        meth = None
        for cls in node.__class__.__mro__:
            meth_name = 'analyse_' + cls.__name__
            meth = getattr(self, meth_name, None)
            if meth:
                break

        if not meth:
            meth = self.analyse_generic
        return meth(node, *args, **kwargs)

    @abstractmethod
    def analyse_generic(self, element): pass

    def update(self, node, *args, **kwargs):
        meth = None
        for cls in node.__class__.__mro__:
            meth_name = 'update_' + cls.__name__
            meth = getattr(self, meth_name, None)
            if meth:
                break

        if not meth:
            meth = self.update_generic
        return meth(node, *args, **kwargs)

    @abstractmethod
    def update_generic(self, element): pass


    @abstractmethod
    def getResult(self): pass

    @abstractmethod
    def done(self): pass


class DCATDMD(Analyser):
    def __init__(self):
        super(DCATDMD, self).__init__()
        self.analysers = dcat_analyser()

    def analyse_Dataset(self, dataset):
        if hasattr(dataset,'dmd'):
            dataset.dmd['dcat'] = {}
        else:
            dataset.dmd={'dcat': {}}
        for id in self.analysers:
            try:
                dataset.dmd['dcat'][id] = self.analysers[id].analyse(dataset)
            except Exception as e:
                ErrorHandler.handleError(log, "DcatAnalyserException", analyser=id, exception=e, exc_info=True)

    def analyse_DCATDMD(self, analyser):
        for id in self.analysers:
            self.analysers[id].analyse(analyser.analysers[id])

    def getResult(self):
        results={}
        for id in self.analysers:
            results[id]=self.analysers[id].getResult()

        return results

    def done(self):
        for id in self.analysers:
            try:
                self.analysers[id].done()
            except Exception as e:
                ErrorHandler.handleError(log, "DcatAnalyserException", analyser=id, exception=e, exc_info=True)

    def update_PortalMetaData(self, pmd):
        if not pmd.qa_stats:
            pmd.qa_stats = {}
        for id in self.analysers:
            try:
                self.analysers[id].update(pmd)
            except Exception as e:
                ErrorHandler.handleError(log, "DcatAnalyserException", analyser=id, exception=e, exc_info=True)

class CKANDMD(Analyser):
    def __init__(self):
        super(CKANDMD, self).__init__()
        self.analysers = ckan_analyser()

    def analyse_Dataset(self,dataset):
        if hasattr(dataset,'dmd'):
            dataset.dmd['ckan'] = {}
        else:
            dataset.dmd={'ckan': {}}
        for id, a in self.analysers:
            try:
                res = a.analyse(dataset)
                if res:
                    dataset.dmd['ckan'][id] = res
            except Exception as e:
                ErrorHandler.handleError(log, "CkanAnalyserException", analyser=id, exception=e, exc_info=True)



def dcat_analyser():
    dcat_analyser = {}
    ##################### EXISTS ######################################
    # ACCESS
    dcat_analyser['ExAc'] = AnyMetric([AccessUrlDCAT(), DownloadUrlDCAT()], id='ExAc')
    # CONTACT
    dcat_analyser['ExCo'] = AnyMetric([DatasetContactDCAT(), DatasetPublisherDCAT()], id='ExCo')


    # DISCOVERY
    dcat_analyser['ExDi'] = AverageMetric([DatasetTitleDCAT(), DatasetDescriptionDCAT(), DatasetKeywordsDCAT(), DistributionTitleDCAT(), DistributionDescriptionDCAT()], id='ExDi')
    # PRESERVATION
    dcat_analyser['ExPr'] = AverageMetric([DatasetAccrualPeriodicityDCAT(), DistributionFormatsDCAT(), DistributionMediaTypesDCAT(), DistributionByteSizeDCAT()], id='ExPr')
    # DATE
    dcat_analyser['ExDa'] = AverageMetric([DatasetCreationDCAT(), DatasetModificationDCAT(), DistributionIssuedDCAT(), DistributionModifiedDCAT()], id='ExDa')


    # LICENSE
    dcat_analyser['ExRi'] = ProvLicenseDCAT()
    # TEMPORAL
    dcat_analyser['ExTe'] = DatasetTemporalDCAT()
    # SPATIAL
    dcat_analyser['ExSp'] = DatasetSpatialDCAT()

    ####################### CONFORMANCE ###########################
    # ACCESS
    dcat_analyser['CoAc'] = AnyConformMetric([ConformAccessUrlDCAT(), ConformDownloadUrlDCAT()], id='CoAc')
    # CONTACT
    dcat_analyser['CoCE'] = AnyConformMetric([EmailConformContactPoint(), EmailConformPublisher()], id='CoCE')
    dcat_analyser['CoCU'] = AnyConformMetric([UrlConformContactPoint(), UrlConformPublisher()], id='CoCU')

    # DATE
    dcat_analyser['CoDa'] = AverageConformMetric([DateConform(dcat_access.getCreationDate),
                                             DateConform(dcat_access.getModificationDate),
                                             DateConform(dcat_access.getDistributionCreationDates),
                                             DateConform(dcat_access.getDistributionModificationDates)], id='CoDa')
    # LICENSE
    dcat_analyser['CoLi'] = LicenseConform()
    # FORMAT
    dcat_analyser['CoFo'] = IANAFormatDCATAnalyser()

    ####################### OPENNESS ###########################
    dcat_analyser['OpFo'] = FormatOpennessDCATAnalyser()
    dcat_analyser['OpMa'] = FormatMachineReadableDCATAnalyser()
    dcat_analyser['OpLi'] = LicenseOpennessDCATAnalyser()

    ####################### RETRIEVABILITY ###########################
    #status_code = DatasetStatusCode()
    #dcat_analyser['ReDa'] = status_code
    #dcat_analyser['ReRe'] = DSRetrieveMetric(status_code)

    return dcat_analyser
