'''
Created on Aug 12, 2015

@author: jumbrich
'''
from odpw.reporting.reporters import UIReporter, CLIReporter, CSVReporter, \
    DBReporter


class SystemEvolutionReporter(DBReporter, UIReporter, CLIReporter, CSVReporter):
    pass

    def getDataFrame(self):
        df = super(SystemEvolutionReporter,self).getDataFrame()
        df =df.fillna(value=0)
        
        
        return df

    def uireport(self):
        df = self.getDataFrame()
        ds=[]
        res=[]
        p=[]
        
        for index, row in df.iterrows():
            sn = row['snapshot']
            soft=row['software']
            
            ds.append({"key":soft, "snapshot":sn, "value":row['datasets']})
            res.append({"key":soft, "snapshot":sn, "value":row['resources']})
            p.append({"key":soft, "snapshot":sn, "value":row['portals']})
            
            
        return {self.name():{'dsevolv':ds,
                'resevolv':res,
                'portalevolv':p
                }}
        
        
        #dslife= {"key": "added_mis_av", "snapshot": 1522, "value": 0},
    
        