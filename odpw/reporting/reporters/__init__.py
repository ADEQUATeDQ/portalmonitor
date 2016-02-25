'''
Created on Jul 22, 2015

@author: jumbrich
'''
from collections import defaultdict

import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt
from odpw.analysers.count_analysers import CKANLicenseCount
from odpw.analysers.fetching import CKANLicenseConformance
from odpw.reporting import graph_plot
import odpw.utils.util as util
import os

import structlog
from abc import abstractmethod
log = structlog.get_logger()

class Reporter(object):

    #def __init__(self):
    #    self.df=None

    def __init__(self, analyser=None):
        self.df=None
        self.a= analyser

    def name(self):
        return self.__class__.__name__.lower()

    @abstractmethod
    def getDataFrame(self):
        pass


def getTopK(self, df, k=10, column=None):
    df = self.getDataFrame()
    self.df['cum_sum'] = df[column].cumsum()
    df['cum_perc'] = 100*df.cum_sum/df[column].sum()

    return df


def dftopk(df, column=None, k=10, percentage=False, otherrow=False):
    rows=df.shape[0]
    dfsort = df.sort(column, ascending=False)

    if rows <k:
        k=rows
    topn = dfsort.copy().head(k)

    if otherrow and k<rows:
        rem = dfsort.copy().tail(rows-k)

        topn= topn.append(rem.sum(numeric_only=True), ignore_index=True)
        topn = topn.replace(np.nan,'others', regex=True)

    if percentage:
        topn= addPercentageCol(topn,column=column)
    return topn

def addPercentageCol(df, column='count', total=None):
    dfc= df.copy()
    tsum = dfc[column].sum()
    if total:
        tsum=total

    if tsum==0:
        dfc['perc'] = dfc[column]
    else:
        dfc['perc'] = dfc[column]/tsum
    return dfc

def DFtoListDict(df):
    d = [
         dict([
               (colname, row[i])
               for i,colname in enumerate(df.columns)
               ])
         for row in df.values
    ]
    return d





class DBReporter(Reporter):



    def getDataFrame(self):
        if self.df is None:
            res = self.a.getResult()
            self.df = pd.DataFrame(res['rows'])
            #self.df.columns = res['columns']
        return self.df


class UIReporter(object):
    def uireport(self):
        return {self.name():DFtoListDict(self.getDataFrame())}



class CSVReporter(object):
    def csvreport(self, folder):
        if not os.path.exists(folder):
            os.makedirs(folder)

        f = os.path.join(folder, self.name()+".csv")
        log.info("CSVReport", file=f, reporter=self.name())

        self._csvreport(f)

        return f

    def _csvreport(self, file):
        df = self.getDataFrame()
        with open(file, "w") as f:
            df.to_csv(f, index=False, encoding='utf-8')

class CLIReporter(object):

    def clireport(self):
        print self.name()
        df = self.getDataFrame()
        print df


class PlotReporter(object):

    def plotreport(self, dir):
        pass



class TexTableReporter(object):

    def textablereport(self, dir):
        pass

class SnapshotsPerPortalReporter(DBReporter,UIReporter,CLIReporter):

    def __init__(self, analyser, portalID):
        super(SnapshotsPerPortalReporter,self).__init__(analyser)
        self.portalID= portalID

    def uireport(self):
        df = self.getDataFrame()
        grouped = df.groupby("portal_id")
        results={}
        for portalID, group in grouped:
            results[portalID]=group['snapshot'].tolist()
        return {'portalSnapshots':results}

    def clireport(self):
        df = self.getDataFrame()
        print df
        grouped = df.groupby("portal_id")
        for portalID, group in grouped:
            print "Snapshots for", portalID
            print group['snapshot'].tolist()


class PortalListReporter(DBReporter,UIReporter,CLIReporter,CSVReporter):
    def __init__(self, analyser):
        super(PortalListReporter,self).__init__(analyser)

    def uireport(self):
        return {'portallist':DFtoListDict(self.getDataFrame())}


class SoftWareDistReporter(DBReporter, UIReporter, CLIReporter,CSVReporter):
    def __init__(self, analyser):
        super(SoftWareDistReporter,self).__init__(analyser)

    def uireport(self):
        return {'softdist':DFtoListDict(addPercentageCol(self.getDataFrame()))}

class ISO3DistReporter(DBReporter,UIReporter,CSVReporter,CLIReporter):
    def __init__(self, analyser):
        super(ISO3DistReporter,self).__init__(analyser)

    def uireport(self):
        df = self.getDataFrame()
        iso3dist=DFtoListDict(addPercentageCol(df))
        iso3Map=[]
        df.set_index("tld")
        for tld, iso3 in util.tld2iso3.items():
            c=0
            if any(df['tld'] == tld):
                d= df[df['tld'] == tld]
                c= df['count'].iloc[d.index.tolist()[0]]
            iso3Map.append({'iso3':iso3,'count':c})

        return {'iso3dist':iso3dist,'iso3Map':iso3Map }

    def _csvreport(self, file):
        df = addPercentageCol(self.getDataFrame())
        with open(file, "w") as f:
            df.to_csv(f,index=False)

class Report(UIReporter,CSVReporter,CLIReporter, PlotReporter, TexTableReporter):

    def __init__(self, reporters):
        self.rs = reporters

    def uireport(self):
        res = {}
        for r in self.rs:
            if isinstance(r, UIReporter):
                s = r.uireport()
                for k, v in s.items():
                    res[k]=v
        return res

    def csvreport(self, folder):
        res = []
        for r in self.rs:
            if isinstance(r, CSVReporter):
                res.append(r.csvreport(folder))

        return res

    def clireport(self):
        for r in self.rs:
            if isinstance(r, CLIReporter):
                r.clireport()

    def plotreport(self, dir):
        for r in self.rs:
            if isinstance(r, PlotReporter):
                r.plotreport(dir)

    def textablereport(self, dir):
        for r in self.rs:
            if isinstance(r, TexTableReporter):
                r.textablereport(dir)


class SystemActivityReporter(Reporter,CLIReporter, UIReporter, CSVReporter):
    def __init__(self,analyser, snapshot=None,portalID=None, dbds=0, dbres=0,dbresproc=0):
        self.analyser = analyser
        self.dbds=dbds
        self.dbres=dbres
        self.snapshot=snapshot
        self.portalID=portalID
        self.dbresproc=dbresproc

        self.df=None

    def getDataFrame(self):
        if  self.df is None:

            res = self.analyser.getResult()
            self.df = pd.DataFrame(res['rows'])

            #self.df.columns = res['columns']
        return self.df

    def uireport(self):
        res = self.analyser.getResult()['summary']

        return {'portalactivitylist':DFtoListDict(self.getDataFrame()),'portalactivitysummary':res, 'dbds':self.dbds, 'dbres':self.dbres,'dbresproc':self.dbresproc}

    def clireport(self):
        summary = self.analyser.getResult()['summary']
        print "System activity report"
        if self.snapshot:
            print "  snapshot:",self.snapshot
        if self.portalID:
            print "  portalID:",self.portalID
        print "--------------"
        print "Fetching"
        #for i in ['done', 'failed', 'running','missing']:
        print "  fetch",summary['fetch']

        print "Resource Headers"
        #for i in ['done','missing']:
        print "  head",summary['head']

class ElementCountReporter(Reporter,CSVReporter,UIReporter, CLIReporter):

    def __init__(self, analyser, columns=None, topK=None,distinct=False):
        super(ElementCountReporter,self).__init__(analyser)
        self.topK=topK
        self.columns=columns
        self.distinct=distinct

    def name(self):
        return self.a.name().lower()

    def getDataFrame(self):
        if self.df is None:
            self.df = pd.DataFrame(self.a.getResult().items(), columns=self.columns)
            if self.topK:
                self.df=dftopk(self.df,column='Count', k=self.topK, percentage=True, otherrow=True)
            else:
                self.df=addPercentageCol(self.df,column='Count')
        return self.df

    def uireport(self):
        res= {self.name(): DFtoListDict(self.getDataFrame())}
        if self.distinct:
            res[self.name()+"_dist"]= len(self.a.getResult().keys())

        print res
        print self.distinct
        return res

    def clireport(self):
        print self.name()
        print '-------------------'
        print self.getDataFrame()
        print

class TagReporter(ElementCountReporter, CSVReporter):
    def __init__(self, analyser, datasetcount,topK=None, distinct=False):
        super(TagReporter, self).__init__(analyser, columns=['Tag', 'Count'], topK=topK,distinct=distinct)
        self.total= datasetcount.getResult()['count']


    def getDataFrame(self):
        #override, since we need to total dataset as extra parameter
        if self.df is None:
            self.df = pd.DataFrame(self.a.getResult().items(), columns=self.columns)
            if self.topK:
                self.df=dftopk(self.df,column='Count', k=self.topK)
            self.df=addPercentageCol(self.df,column='Count', total=self.total)
        return self.df

class OrganisationReporter(ElementCountReporter, CSVReporter):
    def __init__(self, analyser, topK=None, distinct=False):
        super(OrganisationReporter, self).__init__(analyser,columns=['Organisation', 'Count'], topK=topK,distinct=distinct)


class FormatCountReporter(ElementCountReporter, CSVReporter):
    def __init__(self, analyser, topK=None, distinct=False):
        super(FormatCountReporter, self).__init__(analyser,columns=['Format', 'Count'], topK=topK,distinct=distinct)

class LicenseCountReporter(ElementCountReporter, CSVReporter):
    def __init__(self, analyser, topK=None, distinct=False):
        super(LicenseCountReporter, self).__init__(analyser,columns=['License', 'Count'], topK=topK,distinct=distinct)


class LicensesReporter(ElementCountReporter, CSVReporter, UIReporter):
    def __init__(self, licenseCount, licenseConform, topK=None, distinct=False):
        if isinstance(licenseCount, CKANLicenseCount) and isinstance(licenseConform, CKANLicenseConformance):
            self.licenseCount = licenseCount
            self.licenseConform = licenseConform
        super(LicensesReporter,self).__init__(None, topK=topK,distinct=distinct)
        self.df = None

    def getDataFrame(self):
        if self.df is None:
            self.df = pd.DataFrame(self.licenseCount.getResult().items(), columns=['LicenseID', 'Count'])
            self.df['ODConformance'] = self.df['LicenseID'].map(self.licenseConform.getResult())
            self.df=addPercentageCol(self.df, column='Count')
        return self.df

class SumReporter(Reporter, UIReporter):
    def __init__(self, analyser):
        self.analyser= analyser
        self.df = None

    def getDataFrame(self):
        if self.df is None:
            self.df = pd.DataFrame(self.analyser.getResult().items(), columns=['Count','Value'])
            self.df.set_index("Count")

        return self.df

    def uireport(self):
        return {self.name(): DFtoListDict(self.getDataFrame())}

    def clireport(self):
        print self.name()
        print '-------------------'
        print self.getDataFrame()
        print

class DatasetSumReporter(SumReporter):
    pass
class ResourceSumReporter(SumReporter):
    pass

class ResourceCountReporter(Reporter, UIReporter):
    def __init__(self, analyser):
        self.analyser= analyser
        self.df = None

    def getDataFrame(self):
        if self.df is None:
            print self.analyser.getResult()
            self.df = pd.DataFrame([self.analyser.getResult()])
        return self.df

    def uireport(self):
        return {self.name(): self.analyser.getResult()}

class ResourceSizeReporter(Reporter, UIReporter):
    def __init__(self, analyser):
        self.analyser= analyser
        self.df = None

    def getDataFrame(self):
        if self.df is None:
            print self.analyser.getResult()

            self.df = pd.DataFrame([self.analyser.getResult()])
        return self.df

    def uireport(self):
        return {self.name(): self.analyser.getResult()}

    def clireport(self):
        print self.name()
        print '-------------------'
        print self.getDataFrame()
        print
#===============================================================================
#
# class LicensesReporter(Reporter, CSVReporter):
#     def __init__(self, analyser_set):
#         self.analyser = []
#         for a in analyser_set.getAnalysers():
#             if isinstance(a, CKANLicenseCount):
#                 self.analyser.append(a)
#         self.df = None
#
#     def getDataFrame(self):
#         if self.df is None:
#             data = defaultdict(int)
#             conformance = {}
#             for a in self.analyser:
#                 frequ, od_conf = a.getResult()
#                 for k in frequ:
#                     data[k] += frequ[k]
#                     conformance[k] = od_conf[k] if k in od_conf else 'not found'
#             self.df = pd.DataFrame(data.items(), columns=['LicenseID', 'Count'])
#             self.df['OD Conformance'] = self.df['LicenseID'].map(conformance)
#         return self.df
#
#     def csvreport(self, folder):
#         if not os.path.exists(folder):
#             os.makedirs(folder)
#         df = self.getDataFrame()
#
#         with open(os.path.join(folder, "licensesFrequency.csv"), "w") as f:
#             df.to_csv(f, index=False)
#         return os.path.join(folder, "licensesFrequency.csv")
#===============================================================================

class SystemEvolutionReport(Report):

    def uireport(self):
        res = {}
        for r in self.rs:
            if isinstance(r, UIReporter):
                s = r.uireport()

                for k, v in s.items():
                    res[k]=v
        return res

    def getDataFrame(self):
        df = None
        for r in self.rs:
            if df is None:
                df = r.getDataFrame()
                df = df.set_index('snapshot')
            else:
                df1= r.getDataFrame()
                df1 = df1.set_index('snapshot')
                df = df.join(df1)

        return df
    def _csvreport(self, file):
        df = self.getDataFrame()

        with open(file, "w") as f:
            df.to_csv(f, index=False)

    def clireport(self):
        print self.getDataFrame()


class ResourceOverlapReporter(Reporter, PlotReporter, CSVReporter):
    def __init__(self, analyser):
        super(ResourceOverlapReporter, self).__init__()
        self.analyser = analyser
        self.df = None

    def getDataFrame(self):
        if self.df is None:
            nested_dict = self.analyser.getResult()
            self.df = pd.DataFrame(nested_dict).T.fillna(0)
        return self.df

    def plotreport(self, dir):
        graph_plot.draw_graph(self.getDataFrame(), min_node_label=0, min_edge_label=2)

    def _csvreport(self, file):
        df = self.getDataFrame()

        with open(file, "w") as f:
            df.to_csv(f)