'''
Created on Aug 17, 2015

@author: jumbrich
'''
from odpw.reporting.reporters import DBReporter, Report
from pandas import merge
                
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
        return df

class SanityReporter(DBReporter):
    pass