__author__ = 'jumbrich'


import util
import sys
import time

from odpw.timer import Timer
from odpw.db.models import Portal,  PortalMetaData, Dataset, Resource
from odpw.util import getSnapshot,getExceptionCode,ErrorHandler as eh


import math

from urlparse import  urlparse
from collections import defaultdict

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

def cli(args,dbm):
    
    snapshots=[]
    sn = getSnapshot(args)
    if not sn:
        for s in dbm.getSnapshots():
            snapshots.append(s['snapshot'])
    else:
        snapshots.append(sn)
    
    portals=[]
    if args.url:
        p = dbm.getPortal(apiurl=args.url)
        portals.append(p)
    else:
        for res in dbm.getPortals():
            p = Portal.fromResult(dict(res))
            portals.append(p)
    
    for portal in portals:
        print "Sanity check for", portal.id
        for sn in snapshots:
            print "  snapshot",sn
            
            stats={}
            
            pmd = dbm.getPortalMetaData(portalID= portal.id, snapshot=sn)
            stats['pmd']='ok' if pmd else 'missing'
            stats['fetch']='ok' if pmd.fetch_stats and 'fetch_end' in pmd.fetch_stats else 'missing'
            stats['head']='ok' if pmd.res_stats and bool(pmd.res_stats['respCodes']) else 'missing'
            stats['qa']='ok' if pmd.qa_stats else 'missing'
            
            ds=0
            for res in dbm.datasetsPerSnapshot(portalID=portal.id,snapshot=sn):
                ds=res['datasets']
            res=0
            for result in dbm.resourcesPerSnapshot(portalID=portal.id,snapshot=sn):
                res=result['resources']
                
            
            print ds, res, pmd.fetch_stats['datasetsy']
            
            
            
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
    