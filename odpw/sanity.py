from odpw.head_stats import headStats
from odpw.fetch_stats import simulateFetch

__author__ = 'jumbrich'



from pprint import pprint
from odpw.db.models import Portal,  PortalMetaData, Dataset, Resource
from odpw.util import getSnapshot,getExceptionCode,ErrorHandler as eh



import logging
log = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()


def name():
    return 'Sanity'

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
        for res in dbm.getPortals():
            p = Portal.fromResult(dict(res))
            portals.append(p)
    
    
    data={}
    for portal in portals:
        data[portal.id]={'missing':{}, 'snapshots':{}, 'sanity':{}}
        print "Sanity check for", portal.id
        snapshots=[]
        if not sn:
            for s in dbm.getSnapshots(portalID=portal.id):
                snapshots.append(s['snapshot'])
        else:
            snapshots.append(sn)
            
        for sn in snapshots:
            print "  snapshot",sn
            
            data[portal.id]['snapshots'][sn]={'status':{}, 'sanity':{}}
            
            status=data[portal.id]['snapshots'][sn]['status']
            sanity=data[portal.id]['snapshots'][sn]['sanity']
            
            while True:
                pmd = dbm.getPortalMetaData(portalID= portal.id, snapshot=sn)
                if pmd:
                    status['pmd']=True 
                    status['fetch']=True if pmd.fetch_stats and 'fetch_end' in pmd.fetch_stats else False
                    status['head']=True if pmd.res_stats and (bool(pmd.res_stats['respCodes']) or pmd.res_stats['total']==0) else False
                    status['qa']=True if pmd.qa_stats else False
            
                    ds=0
                    for res in dbm.datasetsPerSnapshot(portalID=portal.id,snapshot=sn):
                        ds=res['datasets']
                
                    sanity['fetch_stats']= True if pmd.fetch_stats and pmd.fetch_stats['datasets'] == ds else False
            
                    res=0
                    for result in dbm.resourcesPerSnapshot(portalID=portal.id,snapshot=sn):
                        res=result['resources']
                
                    if status['head']:
                        sanity['res_stats']= True if pmd.res_stats and pmd.res_stats['unique'] == res else False
                        
                    else:
                        sanity['res_stats']= None
                else:
                    status['pmd']=False
                if args.fix:
                    if not status['pmd']:
                        print "Simulating fetch"
                        simulateFetch(portal, dbm, sn)
                    elif not status['head']:
                        print "Computing head stats"
                        headStats(dbm,sn,portal.id)
                    elif not sanity['res_stats']:
                        print "Simulating fetch"
                        simulateFetch(portal, dbm, sn)
                        print "Computing head stats"
                        headStats(dbm,sn,portal.id)
                    else:
                        break
                
                    
            
                
        
        missing = data[portal.id]['missing']
        t=['pmd', 'fetch', 'head','qa']
        for tt in t:
            missing[tt]=[]
        
            for k,v in data[portal.id]['snapshots'].items():
                if tt in v['status'] and not v['status'][tt]:
                        missing[tt].append(k)
            
        t=['fetch_stats','res_stats', 'res_count']
        sanity = data[portal.id]['sanity']
        for tt in t:
            sanity[tt]=[]
            for k,v in data[portal.id]['snapshots'].items():
                if tt in v['sanity'] and not v['sanity'][tt]:
                        sanity[tt].append(k)
        
        pprint(missing)
        pprint(sanity)
            
            
            
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
#         for res in dbm.datasetsPerSnapshot(portalID=portal.id):
#             datasets[res['snapshot']]= res['datasets']
#             if res['snapshot'] not in sn:
#                 sn.append(res['snapshot'])
#             
#         for pmd in dbm.getPortalMetaDatas( portalID=portal.id):
#             pmds[pmd.snapshot]= PortalMetaData.fromResult(dict(pmd))
#             if pmd.snapshot not in sn:
#                 sn.append(pmd.snapshot)
#         
#         for res in dbm.resourcesPerSnapshot(portalID=portal.id):
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
    