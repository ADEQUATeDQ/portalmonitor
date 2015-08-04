'''
Created on Jul 22, 2015

@author: jumbrich
'''
from collections import defaultdict

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from odpw.analysers.fetching import CKANLicenseCount, TagsCount
import odpw.utils.util as util
import os
from odpw.analysers.core import DBAnalyser
from odpw.analysers import AnalyseEngine
from odpw.db.models import PortalMetaData
from odpw.analysers.pmd_analysers import PMDActivityAnalyser

class Reporter(object):
    
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
    
    topn = dfsort.copy().head(k)
    
    if otherrow and k<rows:
        rem = dfsort.copy().tail(rows-k)
        
        topn= topn.append(rem.sum(numeric_only=True), ignore_index=True)
        topn = topn.replace(np.nan,'others', regex=True)
    
    if percentage:
        topn= addPercentageCol(topn)
    return topn

def addPercentageCol(df, column='count'):
    dfc= df.copy()
    dfc['perc'] = 100*dfc[column]/dfc[column].sum()
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
    
    def __init__(self, analyser):
        self.a = analyser
        self.df = None

    def getDataFrame(self):
        if self.df is None:
            res = self.a.getResult()
            self.df = pd.DataFrame(res['rows'])
            self.df.columns = res['columns']
        return self.df

    
class UIReporter(object):
    def uireport(self):
        pass

    
class CSVReporter(object):
    def csvreport(self, folder):
        pass


class CLIReporter(object):
    
    def clireport(self):
        pass

class DataFramePlotReporter(object):

    def plotreport(self):
        pass



class SnapshotsPerPortalReporter(DBReporter,UIReporter,CLIReporter):
    
    def __init__(self, dbm, portalID=None, apiurl=None):
        super(SnapshotsPerPortalReporter,self).__init__(DBAnalyser(dbm.getSnapshots, portalID=portalID,apiurl=None))
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
        grouped = df.groupby("portal_id")
        for portalID, group in grouped:
            print "Snapshots for", portalID
            print group['snapshot'].tolist()
        
    
class SoftWareDistReporter(DBReporter,UIReporter):
    def __init__(self, dbm):
        super(SoftWareDistReporter,self).__init__(DBAnalyser(dbm.getSoftwareDist))
        
    def uireport(self):
        return {'softdist':DFtoListDict(addPercentageCol(self.getDataFrame()))}

class ISO3DistReporter(DBReporter,UIReporter,CSVReporter):
    def __init__(self, dbm):
        super(ISO3DistReporter,self).__init__(DBAnalyser(dbm.getCountryDist))
    
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
    
    def csvreport(self, folder):
        if not os.path.exists(folder):
            os.makedirs(folder)
            
        df = addPercentageCol(self.getDataFrame())
        
        with open(os.path.join(folder,"iso3dist.csv"), "w") as f:
            df.to_csv(f,index=False)
            
        return os.path.join(folder,"iso3dist.csv")
    
    

class Report(UIReporter,CSVReporter,CLIReporter, DataFramePlotReporter):
    
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

    def plotreport(self):
        for r in self.rs:
            if isinstance(r, DataFramePlotReporter):
                r.plotreport()


class SystemActivityReporter(Reporter,CLIReporter, UIReporter):
    def __init__(self,dbm,snapshot=None, portalID=None):
        self.dbm = dbm
        self.snapshot=snapshot
        self.portalID=portalID
        self.df=None
        self.ae = AnalyseEngine()
        self.ae.add(PMDActivityAnalyser())
        
    def run(self):
        iter = self.dbm.getPortalMetaDatas(snapshot=self.snapshot,portalID=self.portalID)
        self.ae.process_all(PortalMetaData.iter(iter))
        
        
    
    def getDataFrame(self):
        if  self.df is None:
            
            res = self.ae.getAnalyser(PMDActivityAnalyser).getResult()
            
            self.df = pd.DataFrame(res['rows'])
            
            #self.df.columns = res['columns']
        return self.df
    
    def uireport(self):
        res = self.ae.getAnalyser(PMDActivityAnalyser).getResult()['summary']
        
        return {'portalactivitylist':DFtoListDict(self.getDataFrame()),'summary':res}
    
    def clireport(self):
        
        summary = self.ae.getAnalyser(PMDActivityAnalyser).getResult()['summary']
        print "System activity report"
        if self.snapshot:
            print "  snapshot:",self.snapshot
        if self.portalID:
            print "  portalID:",self.portalID
        print "--------------"
        print "Fetching"
        for i in ['done', 'failed', 'running','missing']: 
            print "  ",i,'-',summary['fetch_'+i]
            
        print "Resource Headers"
        for i in ['done','missing']: 
            print "  ",i,'-',summary['head_'+i]
    
class TagReporter(Reporter, CSVReporter, DataFramePlotReporter):
    def __init__(self, analyser_set):
        self.analyser = []
        for a in analyser_set.getAnalysers():
            if isinstance(a, TagsCount):
                self.analyser.append(a)
        self.df = None

    def getDataFrame(self):
        if self.df is None:
            data = defaultdict(int)
            for a in self.analyser:
                res = a.getResult()
                for k in res:
                    data[k] += res[k]
            self.df = pd.DataFrame(data.items(), columns=['Tag', 'Count'])
        return self.df

    def csvreport(self, folder):
        if not os.path.exists(folder):
            os.makedirs(folder)
        df = self.getDataFrame()

        with open(os.path.join(folder, "tagsFrequency.csv"), "w") as f:
            df.to_csv(f, index=False)
        return os.path.join(folder, "tagsFrequency.csv")

    def plotreport(self):
        df = self.getDataFrame().copy()
        p = df.plot()
        plt.show()

        x=np.linspace(-10,10,201)
        y,z=np.sin(x),np.cos(x)
        x,y,z=pd.Series(x),pd.Series(y),pd.Series(z)
        df=pd.concat([x,y,z],axis=1)
        df.columns=['x','sin(x)','cos(x)']
        df=df.set_index('x')
        df.plot()
        plt.show()

        pass


class LicensesReporter(Reporter, CSVReporter):
    def __init__(self, analyser_set):
        self.analyser = []
        for a in analyser_set.getAnalysers():
            if isinstance(a, CKANLicenseCount):
                self.analyser.append(a)
        self.df = None

    def getDataFrame(self):
        if self.df is None:
            data = defaultdict(int)
            conformance = {}
            for a in self.analyser:
                frequ, od_conf = a.getResult()
                for k in frequ:
                    data[k] += frequ[k]
                    conformance[k] = od_conf[k] if k in od_conf else 'not found'
            self.df = pd.DataFrame(data.items(), columns=['LicenseID', 'Count'])
            self.df['OD Conformance'] = self.df['LicenseID'].map(conformance)
        return self.df

    def csvreport(self, folder):
        if not os.path.exists(folder):
            os.makedirs(folder)
        df = self.getDataFrame()

        with open(os.path.join(folder, "licensesFrequency.csv"), "w") as f:
            df.to_csv(f, index=False)
        return os.path.join(folder, "licensesFrequency.csv")
