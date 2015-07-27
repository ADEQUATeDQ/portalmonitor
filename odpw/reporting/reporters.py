'''
Created on Jul 22, 2015

@author: jumbrich
'''

import pandas
import numpy as np

class DBAnalyser(object):
    
    def __init__(self, func):
        self.func=func
        self.rows=[]
        self.df=None
        self.columns=None
        
    def analyse(self):
        
        res = self.func()
        self.columns=res.keys()
        for r in res:
            self.rows.append(r)
        
    def getDataFrame(self):
        if not self.df:
            self.df = pandas.DataFrame(self.rows)
            self.df.columns = self.columns

        return self.df

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

def addPercentageCol(df):
    dfc= df.copy()
    dfc['perc'] = 100*dfc['count']/dfc['count'].sum()
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


    
        