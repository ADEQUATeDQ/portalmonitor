'''
Created on Aug 17, 2015

@author: jumbrich
'''
from _collections import defaultdict

import pandas as pd
from pandas import merge

from odpw.reporting.reporters import DBReporter, Report


class SanityReport(Report):
    def getDataFrame(self):
        df = None
        for r in self.rs:
            if df is None:
                df = r.getDataFrame()
                #df =df.set_index(['snapshot','portalID'])
                #print df
            else:
                df1= r.getDataFrame()
                #df1 = df1.set_index(['snapshot','portalID'])

                #print df1
                df = merge(df,df1, on=['snapshot','portalID'])
        df1 = df.where((pd.notnull(df)), None)
        return df1

    def _csvreport(self, file):
        df =self.getDataFrame()
        df.to_csv(file)
        
    def clireport(self):
        df=self.getDataFrame()
        f_ok=defaultdict(int)
        h_ok=defaultdict(int)
        f_mis=defaultdict(int)
        f_failed=defaultdict(int)
        h_mis=defaultdict(int)
        h_failed=defaultdict(int)
        
        for index, row in df.iterrows():
            print row['portalID'],row['snapshot']
            #print row, row['head_processed'] is None
            if row['fetch_processed'] is True:
                print "\t[O]- Fetch process"
                if row['fetch_exception']:
                    print "\t[O]- fetch_exception"
            elif row['fetch_processed'] is None:
                print "\t[M]- Fetch process"
            else:
                print "\t[X]- Fetch process"
                for k,v in row.iteritems():
                    if k.startswith("fetch_") and not v and k != 'fetch_processed':
                        print "\t", '[O]-' if v else '[X]-', k
                        
            
            if row['head_processed'] is True:
                print "\t[O]- Head process"
            elif row['head_processed'] is None:
                print "\t[M]- Head process"
            else:
                print "\t[X]- Head process"
                for k,v in row.iteritems():
                    if k.startswith("head_") and not v and k != 'head_processed':
                        print "\t", '[O]-' if v else '[X]-', k
            
            for k,v in row.iteritems():
                if v is True:
                    if k.startswith("fetch_"):
                        f_ok[k]+=1
                    elif k.startswith("head_"):
                        h_ok[k]+=1
                elif v is None:
                    if k.startswith("fetch_"):
                        f_mis[k]+=1
                    elif k.startswith("head_"):
                        h_mis[k]+=1
                else:
                    if k.startswith("fetch_"):
                        f_failed[k]+=1
                    elif k.startswith("head_"):
                        h_failed[k]+=1
                    
        
        
        print "\n/-------- SUMMARY ---------"
        print "| PORTALS: ",df.shape[0]
        for ok in [f_ok, h_ok]:
            if len(ok) >0:
                for k,v in ok.items():
                    print "|\t[O]",k,v
        for failed in [f_failed, h_failed]:
            if len(failed) >0:
                print "|\n|----FAILED"
                for k,v in failed.items():
                    print "|\t[X]",k,v
        for mis in [f_mis, h_mis]:
            if len(mis)>0:
                print "|\n|----MISSING"
                for k,v in mis.items():
                    print "|\t[M]",k,v
        print "\\"+"-"*40

class SanityReporter(DBReporter):
    pass