from odpw.analysers.sanity import FetchSanity, HeadSanity
from odpw.analysers import AnalyserSet, process_all

__author__ = 'jumbrich'

from odpw.db.models import Portal
from odpw.utils.util import getSnapshot
import pandas as pd

from odpw.utils.head_stats import headStats
from odpw.utils.fetch_stats import simulateFetching


import structlog
log =structlog.get_logger()


def name():
    return 'Sanity'
def help():
    return "Check the sanity of the system"
def setupCLI(pa):
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")

    pa.add_argument("-f","--fix",  help='try to fix missing steps', dest='fix', action='store_true')

def cli(args,dbm):
    sn = getSnapshot(args)
    
    portals=[]
    if args.url:
        p = dbm.getPortal(apiurl=args.url)
        portals.append(p)
    else:
        for p in Portal.iter(dbm.getPortals()):
            portals.append(p)
    
    df = pd.DataFrame()
    for portal in portals:
        print "Sanity check for", portal.id
        snapshots=[]
        if not sn:
            for s in dbm.getSnapshots(portalID=portal.id):
                snapshots.append(s['snapshot'])
        else:
            snapshots.append(sn)
            
        for sn in snapshots:
            print "  snapshot",sn
            
            stats={'snapshot':sn, 'portal':portal.id, 'portal_ds':None,'portal_res':None}
            
            checkagain=True
            
            
            
            while checkagain:
                pmd = dbm.getPortalMetaData(portalID= portal.id, snapshot=sn)
            
                aset= AnalyserSet()
                fs= aset.add(FetchSanity(dbm))
                rs= aset.add(HeadSanity(dbm))
                
                process_all(aset, [pmd])
                
                if not fs.getResult()['processed'] or  not rs.getResult()['processed_fetch']:
                    print " Fetch simulation is required"
                if not rs.getResult()['processed_head']:
                    print " head is required"
                    
                     
                if args.fix:
                    
                    
                    if not stats['fetch_process']:
                        print "Simulating fetch because of missing PMD"
                        simulateFetching(dbm,portal,  sn)
                    #===========================================================
                    # if not status['pmd']:
                    #     print "Simulating fetch because of missing PMD"
                    #     simulateFetching(dbm,portal,  sn)
                    # elif not sanity['fetch_stats_head']:
                    #     print "Simulating fetch because of inconsistent total/unqiue"
                    #     simulateFetching(dbm,portal,  sn)
                    # 
                    # elif not status['head']:
                    #     print "Computing head stats"
                    #     headStats(dbm,sn,portal.id)
                    # elif not sanity['res_stats']:
                    #     print "Simulating fetch"
                    #     simulateFetching( dbm,portal, sn)
                    #     print "Computing head stats"
                    #     headStats(dbm,sn,portal.id)
                    # else:
                    #     checkagain=False
                    #===========================================================
                    checkagain=False
                else:
                    checkagain=False
            
            
            print 'stats',stats
            df=df.append(stats,ignore_index=True)
            
        #print df
        
        #missing = data[portal.id]['missing']
        #t=['pmd', 'fetch', 'head','qa']
        #for tt in t:
        #    missing[tt]=[]
       # 
        #    for k,v in data[portal.id]['snapshots'].items():
        #        if tt in v['status'] and not v['status'][tt]:
        #                missing[tt].append(k)
        #    
        #t=['fetch_stats','fetch_stats_head','res_stats', 'res_count']
        #sanity = data[portal.id]['sanity']
        #for tt in t:
        #    sanity[tt]=[]
        #    for k,v in data[portal.id]['snapshots'].items():
        #        if tt in v['sanity'] and not v['sanity'][tt]:
        #                sanity[tt].append(k)
        #
        #pprint(missing)
        #pprint(sanity)
            
    
    df.to_csv("sanity.csv")
    
    
    
            
#===============================================================================
#     
#     
#     
#         log.info('Sanity check', portalID=portal.id)
#         
#         stats={}
#         datasets={}
#         pmds={}
#         resources={}
#         sn=[]
#         #check the DB for datasets
#         for res in dbm.countDatasetsPerSnapshot(portalID=portal.id):
#             datasets[res['snapshot']]= res['datasets']
#             if res['snapshot'] not in sn:
#                 sn.append(res['snapshot'])
#             
#         for pmd in dbm.getPortalMetaDatas( portalID=portal.id):
#             pmds[pmd.snapshot]= PortalMetaData.fromResult(dict(pmd))
#             if pmd.snapshot not in sn:
#                 sn.append(pmd.snapshot)
#         
#         for res in dbm.countResourcesPerSnapshot(portalID=portal.id):
#             resources[res['snapshot']]= res['resources']
#             if res['snapshot'] not in sn:
#                 sn.append(res['snapshot'])
#             
#         
#         
#         portal_status[portal.id]={
#                                   'missing_pmds': list(set(sn) - set(pmds.keys())),
#                                   'missing_ds': list(set(sn) - set(datasets.keys())),
#                                   'missing_res': list(set(sn) - set(resources.keys()))
#                                   }
# ##figure out for which snapshots we have datasets, resources and pmds
#         portal_status[portal.id]['inconsistent_ds']=0
#         portal_status[portal.id]['inconsistent_res']=0
#         for pmd_sn in pmds:
#             pmd= pmds[pmd_sn]
#             if pmd.snapshot in datasets and pmd.datasets != int(datasets[pmd.snapshot]):
#                 portal_status[portal.id]['inconsistent_ds']+=1
#             if pmd.snapshot in resources and pmd.resources != resources[pmd.snapshot]:
#                 portal_status[portal.id]['inconsistent_res']+=1    
#         
#     for portal in portal_status:
#         print 'Portal', portal
#         print '  missing_pmds', portal_status[portal]['missing_pmds']
#         print '  missing_ds', portal_status[portal]['missing_ds']
#         print '  missing_res', portal_status[portal]['missing_res']
#         print '  inconsistent_ds', portal_status[portal]['inconsistent_ds']
#         print '  inconsistent_res', portal_status[portal]['inconsistent_res']
#     
#     print 'Summary'
#     for portal in portal_status:
#         print 'Portal', portal
#         print '  missing_pmds', len(portal_status[portal]['missing_pmds'])
#         print '  missing_ds', len(portal_status[portal]['missing_ds'])
#         print '  missing_res', len(portal_status[portal]['missing_res'])
#         print '  inconsistent_ds', portal_status[portal]['inconsistent_ds']
#         print '  inconsistent_res', portal_status[portal]['inconsistent_res']
#===============================================================================
    