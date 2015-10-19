'''
Created on Aug 17, 2015

@author: jumbrich
'''

from odpw.db.models import Dataset, DatasetLife, Portal
from odpw.analysers.core import DCATConverter
from odpw.analysers.fetching import DCATDatasetAge
from odpw.utils.util import getSnapshot, progressIterator, ErrorHandler,\
    tofirstdayinisoweek, getSnapshotfromTime, weekIter
from _collections import defaultdict
from odpw.db.dbm import PostgressDBM

import dateutil

from datetime import datetime, timedelta, date
from multiprocessing.pool import ThreadPool
import multiprocessing

import structlog
import time
from _functools import partial
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.datasetlife import DatasetLifeAnalyser
log =structlog.get_logger()




def compute_dataset_life(dbm, job):
    try: 
        Portal = job['Portal']
        sn = job['snapshot']
    
        log.info("START DatasetLife Fetch", pid=Portal.id, snapshot=sn, software=Portal.software)
    
        dbm.engine.dispose()
    
        aset = AnalyserSet()
    
        aset.add(DCATConverter(Portal))
        aset.add(DatasetLifeAnalyser(dbm))
    
        iter = Dataset.iter(dbm.getDatasets(portalID=Portal.id, snapshot=sn))
        total = dbm.countDatasets(portalID=Portal.id, snapshot=sn)
    
        steps = total/10 if total>10 else 1
        process_all(aset, progressIterator(iter, total, steps, label=Portal.id))
    except Exception as e:
        ErrorHandler.handleError(log, "DatasetLifeException", portal=Portal.id, snapshot=sn)
    

def help():
    return "Update datasetlife statistics"

def name():
    return 'DatasetLife'

def setupCLI(pa):
    
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=1)
    pa.add_argument("-ns","--nosnap",  help='no snapshot', dest='snapshotignore', action='store_true')
    
def cli(args,dbm):
    sn = getSnapshot(args)
    
    portals=[]
    if args.url:
        p = dbm.getPortal(apiurl=args.url)
        if p:
            portals.append(p)
    else:
        for p in Portal.iter(dbm.getPortals()):
            portals.append(p)
    
    
    
    jobs=[]
    for p in portals:
        snapshots=set([])
        if not sn:
            for s in dbm.getSnapshots(portalID=p.id):
                snapshots.add(s['snapshot'])
        else:
            snapshots.add(sn)
        
        for sn in sorted(snapshots):
            jobs.append({'Portal':p, 'snapshot':sn})
            
    
    
    pool = ThreadPool(processes=args.processors,) 
    mgr = multiprocessing.Manager()
    seen = mgr.dict()
    
    #for pmd in PortalMetaData.iter(dbm.getPortalMetaDatas(snapshot=sn)):
    #    seen[pmd.portal_id]= {'resources':pmd.resources, 'processed':0}
    
    log.info("Starting fetch sim lookups", count=len(portals), cores=args.processors)
    
    head_star = partial(compute_dataset_life,dbm)
    
    start = time.time()
    results = pool.imap_unordered(head_star, jobs)
    pool.close()
    
    c=0
    total=len(jobs)
    steps= total/100 if total/100 !=0 else 1
    
    #for res in results:
    #    c+=1
    #    if c%steps==0:
    #        elapsed = (time.time() - start)
    #        progressIndicator(c, total, elapsed=elapsed, label="Fetch Simulate Progress")
   
    #progressIndicator(c, total, elapsed=elapsed, label="Fetch Simulate Progress")
    pool.join()
    
    
    
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
                  
                
            
             