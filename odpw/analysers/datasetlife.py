'''
Created on Aug 18, 2015

@author: jumbrich
'''
from odpw.analysers import Analyser
from odpw.db.models import DatasetLife, PortalMetaData
from odpw.utils.dataset_converter import DCAT, DCT
import datetime
import dateutil

from datetime import datetime, timedelta, date
from _collections import defaultdict

def weekIter(startDate, endDate, delta=timedelta(days=7)):
    currentDate = startDate
        
    while currentDate < endDate:
        yield getSnapshotfromTime(currentDate)
        currentDate += delta

def tofirstdayinisoweek(yearweek):
    year=int('20'+str(yearweek)[:2])
    week=int(str(yearweek)[2:])
    ret = datetime.strptime('%04d-%02d-1' % (year, week), '%Y-%W-%w')
    if date(year, 1, 4).isoweekday() > 4:
        ret -= timedelta(days=7)
    return ret

def getSnapshotfromTime(now, delta=None,before=False):
    if delta:
        if before:
            now -= delta
        else:
            now += delta
            
    y=now.isocalendar()[0]
    w=now.isocalendar()[1]
    sn=str(y)[2:]+'{:02}'.format(w)
    return int(sn)


class DatasetLifeStatsAnalyser(Analyser):
    
    def __init__(self, dbm, snapshot, portal):
        self.snasphot = snapshot
        self.counts={}
        
        self.pmd_snapshots=[]
        for res in dbm.getSnapshotsFromPMD(portalID=portal.id):
            self.pmd_snapshots.append(res['snapshot'])
        self.pmd_snapshots= sorted(self.pmd_snapshots)
        
        self.pmd_startDate= tofirstdayinisoweek(sorted(self.pmd_snapshots)[0])
        self.pmd_endDate= tofirstdayinisoweek(sorted(self.pmd_snapshots)[-1])
        
        self.toupdate=set([])
    
    def analyse_DatasetLife(self,df):
        
        #get information
        snapshots = df.snapshots
         
        if len(snapshots)>1:
            print df.id, len(snapshots)
        else:
            created = snapshots.iterkeys().next()
            ds_snap = snapshots.itervalues().next()
            
            ds_start=sorted(ds_snap)[0]
            ds_end=sorted(ds_snap)[-1]
            
            created=getSnapshotfromTime(dateutil.parser.parse(created))
            if created > ds_start: 
                # that is stange, we have the ds in the DB, ut the creation date was updated
                created=getSnapshotfromTime(tofirstdayinisoweek(ds_start), delta=timedelta(days=7), before=True)
                
                
            #if created!=ds_start:
            #    created_n=getSnapshotfromTime(datasetLife[ds]['created'][0], delte=timedelta(days=7))
            #    if created_n!=ds_start:
            #        print "DAMN, created is not indexed", created_n, ds_start
            #    else:
            #        created=created_n
            #print ds_start, ds_end, created
            
            for pmd_sn in weekIter(self.pmd_startDate, self.pmd_endDate):
                if pmd_sn not in self.counts:
                    self.counts[pmd_sn]=defaultdict(int)
                self.counts[pmd_sn]['pmd']= pmd_sn in snapshots
                
                if pmd_sn < created:
                    #DS did not exist for this pmd sn
                    pass
                else:
                    if pmd_sn in ds_snap:
                        if pmd_sn == created: 
                            self.counts[pmd_sn]['added_accessed']+=1
                        else:
                            self.counts[pmd_sn]['accessed']+=1
                            if pmd_sn == self.snasphot:
                                #get prev ds snapshot
                                if ds_snap.index(pmd_sn)>=1:
                                    ds_prev_sn= ds_snap[ds_snap.index(pmd_sn)-1]
                                    #get one week back
                                    prev_week= getSnapshotfromTime(tofirstdayinisoweek(self.snasphot), delta=timedelta(days=7), before=True)
                                    if ds_prev_sn != prev_week: 
                                        start= tofirstdayinisoweek(ds_prev_sn)
                                        end=tofirstdayinisoweek(prev_week)
                                        for sn in weekIter(start,end):
                                            self.toupdate.add(sn)
                                        ##check if we need to update the older snapshots
                            
                    elif pmd_sn == created:
                        self.counts[pmd_sn]['added_mis_av']+=1
                    elif ds_start <= pmd_sn <=ds_end:
                        self.counts[pmd_sn]['mis_av']+=1
                    elif pmd_sn > ds_end:
                        self.counts[pmd_sn]['dead']+=1
    
    
    def done(self):
        print self.toupdate
    
    def getResult(self):
        return self.counts
    
    def update_PortalMetaData(self, pmd):
        if not pmd.fetch_stats:
            pmd.fetch_stats={} 
        for k, v in dict(self.getResult()[self.snasphot]).items():
            if k != 'pmd': 
                pmd.fetch_stats[k]=v
        

class DatasetLifeAnalyser(Analyser):
    
    def __init__(self, dbm):
        super(DatasetLifeAnalyser, self).__init__()
        self.dbm=dbm
    
    def analyse_Dataset(self, dataset):
            
            did= dataset.id
            df = self.dbm.getDatasetLife(id=did, portalID= dataset.portal_id)
            insert=False
            if df is None:
                insert=True
                df = DatasetLife(did=did, portalID= dataset.portal_id)
                 
            for dcat_el in getattr(dataset,'dcat',[]):
                if str(DCAT.Dataset) in dcat_el.get('@type',[]):
                    for f in dcat_el.get(str(DCT.issued),[]):
                        try:
                            created = datetime.datetime.strptime(f['@value'].split(".")[0], "%Y-%m-%dT%H:%M:%S")
                            self.ages['created'].append(created)
                            break
                        except Exception as e:
                            pass
            if created:
                df.updateSnapshot(created.isoformat(),dataset.snapshot )
                if insert:
                    self.dbm.insertDatasetLife(df)
                else:
                    self.dbm.updateDatasetLife(df)
            else:
                print 'No creation date', dataset.portal_id, dataset.id, dataset.snapshot