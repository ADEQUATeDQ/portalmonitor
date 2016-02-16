'''
Created on Aug 17, 2015

@author: jumbrich
'''
from odpw.db.dbm import PostgressDBM
from odpw.db.models import Dataset, DatasetLife
from odpw.analysers.core import DCATConverter
from odpw.analysers.fetching import DCATDatasetAge
from odpw.utils.util import getSnapshot
from _collections import defaultdict

import dateutil

from datetime import datetime, timedelta, date


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

if __name__ == '__main__':
    start = 1448
    end=1524
    
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    #dbm.initDatasetsLife()
    
    portalID='data_wu_ac_at'
    snapshots=[]
    for res in dbm.getSnapshotsFromPMD(portalID=portalID):
        snapshots.append(res['snapshot'])
         
    snapshots= sorted(snapshots)
     
    #===========================================================================
    # dcat= DCATConverter(dbm.getPortal(portalID=portalID))
    #  
    # for sn in snapshots:
    #     for d in Dataset.iter(dbm.getDatasets(portalID=portalID, snapshot=sn)):
    #         dcat.analyse_Dataset(d)
    #          
    #         did= d.id
    #         
    #         df = dbm.getDatasetLife(id=did, portalID= d.portal_id)
    #         insert=False
    #         if df is None:
    #             insert=True
    #             df = DatasetLife(did=did, portalID= d.portal_id)
    #             
    #         #datasetLife[did]['sn'].add(sn)
    #          
    #         dage= DCATDatasetAge()
    #         dage.analyse_Dataset(d)
    #          
    #         if len(dage.ages['created'])==1:
    #             created=dage.ages['created'][0]
    #             df.updateSnapshot(created.isoformat(),sn )
    #             if insert:
    #                 dbm.insertDatasetLife(df)
    #             else:
    #                 dbm.updateDatasetLife(df)
    #===========================================================================
     
         
    counts={}
    startDate= tofirstdayinisoweek(sorted(snapshots)[0])
    endDate= tofirstdayinisoweek(sorted(snapshots)[-1])
    print startDate, endDate
    
    for df in DatasetLife.iter(dbm.getDatasetLifeResults(portalID=portalID)):
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
            print ds_start, ds_end, created
            
            for pmd_sn in weekIter(startDate, endDate):
                if pmd_sn not in counts:
                    counts[pmd_sn]=defaultdict(int)
                
                
                
                counts[pmd_sn]['pmd']= pmd_sn in snapshots
                if pmd_sn < created:
                    #DS did not exist for this pmd sn
                    pass
                else:
                    
                    if pmd_sn in ds_snap:
                        if pmd_sn == created: 
                            counts[pmd_sn]['added_accessed']+=1
                        else:
                            counts[pmd_sn]['accessed']+=1
                    elif pmd_sn == created:
                        counts[pmd_sn]['added_mis_av']+=1
                    elif ds_start <= pmd_sn <=ds_end:
                        counts[pmd_sn]['mis_av']+=1
                    elif pmd_sn > ds_end:
                        counts[pmd_sn]['dead']+=1
                    
    for s, d in counts.items():
        print s, d
                  
                
            
             