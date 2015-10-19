'''
Created on Aug 18, 2015

@author: jumbrich
'''
from odpw.analysers import Analyser
from odpw.db.models import DatasetLife, PortalMetaData
from odpw.utils.dataset_converter import DCAT, DCT

import dateutil

from datetime import datetime, timedelta, date
from _collections import defaultdict
from odpw.utils.util import tofirstdayinisoweek, getSnapshotfromTime, weekIter
from odpw.utils.dcat_access import getCreationDate







class DatasetLifeStatsAnalyser(Analyser):
    
    ADDACC='added_accessed'
    ACC='accessed'
    ADDMISAV='added_mis_av'
    MISAV='mis_av'
    DEAD='dead'
    
    keys=[ADDACC, ACC, ADDMISAV, MISAV, DEAD]
    
    def __init__(self, dbm, snapshot, portal):
        self.snasphot = snapshot
        
        self.counts={self.ACC:0,
                     self.ADDACC:0,
                     self.ADDMISAV:0,
                     self.MISAV:0,
                     self.DEAD:0}
        
        #=======================================================================
        # self.pmd_snapshots=[]
        # for res in dbm.getSnapshotsFromPMD(portalID=portal.id):
        #     self.pmd_snapshots.append(res['snapshot'])
        # self.pmd_snapshots= sorted(self.pmd_snapshots)
        # 
        # self.pmd_startDate= tofirstdayinisoweek(sorted(self.pmd_snapshots)[0])
        # self.pmd_endDate= tofirstdayinisoweek(sorted(self.pmd_snapshots)[-1])
        # 
        # self.toupdate=set([])
        #=======================================================================
    
    def analyse_DatasetLife(self,df):
        try:
        
            #get information
            created = sorted(df.snapshots['created'])[0]
            
            sns=df.snapshots['indexed']
            #sort sn tuples by start date
            sns=sorted(sns, key=lambda tup: tup[0])
                
            ds_start = sns[0][0]
            ds_end= sns[-1][1]
                
            
            idx=False
            for t in sns:
                if t[0] <= self.snasphot <= t[1]:
                    #snapshot is between/equals start and end
                    #-> we accessed the dataset
                    if created == self.snasphot:
                        self.counts[ self.ADDACC ]+=1
                        idx=True
                    else:
                        self.counts[self.ACC]+=1
                        idx=True
        
            if not idx:
                if ds_start <= self.snasphot <= ds_end:
                    if self.snasphot == created:
                        self.counts[  self.ADDMISAV ]+=1
                    else:
                        self.counts[self.MISAV]+=1
                    # snapshot is in the lifespan of the ds but not indexed
                if self.snasphot > ds_end:
                    self.counts[self.DEAD]+=1
        except Exception as e:
            print e
        
    #===========================================================================
    #     #created = snapshots.iterkeys().next()
    #     #ds_snap = snapshots.itervalues().next()
    #     
    #     
    #     created=getSnapshotfromTime(dateutil.parser.parse(created))
    #     if created > ds_start: 
    #         # that is stange, we have the ds in the DB, ut the creation date was updated
    #         created=getSnapshotfromTime(tofirstdayinisoweek(ds_start), delta=timedelta(days=7), before=True)
    #         
    #         
    #     
    #     for pmd_sn in weekIter(self.pmd_startDate, self.pmd_endDate):
    #         if pmd_sn not in self.counts:
    #             self.counts[pmd_sn]=defaultdict(int)
    #         self.counts[pmd_sn]['pmd']= pmd_sn in snapshots
    #         
    #         if pmd_sn < created:
    #             #DS did not exist for this pmd sn
    #             pass
    #         else:
    #             if pmd_sn in ds_snap:
    #                 if pmd_sn == created: 
    #                     self.counts[ pmd_sn ][ self.ADDACC ]+=1
    #                 else:
    #                     self.counts[pmd_sn][ self.ACC ]+=1
    #                     if pmd_sn == self.snasphot:
    #                         #get prev ds snapshot
    #                         if ds_snap.index(pmd_sn)>=1:
    #                             ds_prev_sn= ds_snap[ds_snap.index(pmd_sn)-1]
    #                             #get one week back
    #                             prev_week= getSnapshotfromTime(tofirstdayinisoweek(self.snasphot), delta=timedelta(days=7), before=True)
    #                             if ds_prev_sn != prev_week: 
    #                                 start= tofirstdayinisoweek(ds_prev_sn)
    #                                 end=tofirstdayinisoweek(prev_week)
    #                                 for sn in weekIter(start,end):
    #                                     self.toupdate.add(sn)
    #                                 ##check if we need to update the older snapshots
    #                     
    #             elif pmd_sn == created:
    #                 self.counts[pmd_sn][  self.ADDMISAV ]+=1
    #             elif ds_start <= pmd_sn <=ds_end:
    #                 self.counts[pmd_sn][self.MISAV]+=1
    #             elif pmd_sn > ds_end:
    #                 self.counts[pmd_sn][self.DEAD]+=1
    # 
    #===========================================================================
    
    def getResult(self):
        return self.counts
    
    def update_PortalMetaData(self, pmd):
        if not pmd.fetch_stats:
            pmd.fetch_stats={} 
        for k, v in dict(self.getResult()).items():
            if k != 'pmd': 
                pmd.fetch_stats[k]=v
        

class DatasetLifeAnalyser(Analyser):
    
    def __init__(self, dbm):
        super(DatasetLifeAnalyser, self).__init__()
        self.dbm=dbm
    
    def analyse_Dataset(self, dataset):
        try:
            did= dataset.id
            
            #check if we have this dataset already indexed
            df = self.dbm.getDatasetLife(id=did, portalID= dataset.portal_id)
            insert=False
            if df is None:
                insert=True
                df = DatasetLife(did=did, portalID= dataset.portal_id)
            
            created=None
            
            c_date=getCreationDate(dataset)
            if len(c_date) >0:
                try:
                    created = datetime.strptime(c_date[0].split(".")[0], "%Y-%m-%dT%H:%M:%S")
                except Exception as e:
                    try:
                        created = datetime.strptime(c_date[0].split(".")[0], "%Y-%m-%d")
                    except Exception as e:
                        created = datetime(2014, 6, 1)
            else:
                created = datetime(2014, 6, 1)
                #print 'No creation date', dataset.portal_id, dataset.id, dataset.snapshot
            
            df.updateSnapshot(created, dataset.snapshot )
            if insert:
                self.dbm.insertDatasetLife(df)
            else:
                self.dbm.updateDatasetLife(df) 
        except Exception as e:
            print e           