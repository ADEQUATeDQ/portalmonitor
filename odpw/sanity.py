__author__ = 'jumbrich'

from util import getSnapshot,getExceptionCode,ErrorHandler as eh

import util
import sys
import time
from timer import Timer

import math
from db.models import Portal,  PortalMetaData, Dataset, Resource
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
    pass

def cli(args,dbm):
    
    portal_status={}
    
    for res in dbm.getPortals():
        portal = Portal.fromResult(dict(res))
        log.info('Sanity check', portalID=portal.id)
        
        stats={}
        datasets={}
        pmds={}
        resources={}
        sn=[]
        #check the DB for datasets
        for res in dbm.datasetsPerSnapshot(portalID=portal.id):
            datasets[res['snapshot']]= res['datasets']
            if res['snapshot'] not in sn:
                sn.append(res['snapshot'])
            
        for pmd in dbm.getPortalMetaDatas( portalID=portal.id):
            pmds[pmd.snapshot]= PortalMetaData.fromResult(dict(pmd))
            if pmd.snapshot not in sn:
                sn.append(pmd.snapshot)
        
        for res in dbm.resourcesPerSnapshot(portalID=portal.id):
            resources[res['snapshot']]= res['resources']
            if res['snapshot'] not in sn:
                sn.append(res['snapshot'])
            
        portal_status[portal.id]={
                                  'missing_pmds': list(set(sn) - set(pmds.keys())),
                                  'missing_ds': list(set(sn) - set(datasets.keys())),
                                  'missing_res': list(set(sn) - set(resources.keys()))
                                  }
##figure out for which snapshots we have datasets, resources and pmds
        portal_status[portal.id]['inconsistent_ds']=0
        portal_status[portal.id]['inconsistent_res']=0
        for pmd_sn in pmds:
            pmd= pmds[pmd_sn]
            if pmd.snapshot in datasets and pmd.datasets != int(datasets[pmd.snapshot]):
                portal_status[portal.id]['inconsistent_ds']+=1
            if pmd.snapshot in resources and pmd.resources != resources[pmd.snapshot]:
                portal_status[portal.id]['inconsistent_res']+=1    
        
    for portal in portal_status:
        print 'Portal', portal
        print '  missing_pmds', portal_status[portal]['missing_pmds']
        print '  missing_ds', portal_status[portal]['missing_ds']
        print '  missing_res', portal_status[portal]['missing_res']
        print '  inconsistent_ds', portal_status[portal]['inconsistent_ds']
        print '  inconsistent_res', portal_status[portal]['inconsistent_res']
    
    print 'Summary'
    for portal in portal_status:
        print 'Portal', portal
        print '  missing_pmds', len(portal_status[portal]['missing_pmds'])
        print '  missing_ds', len(portal_status[portal]['missing_ds'])
        print '  missing_res', len(portal_status[portal]['missing_res'])
        print '  inconsistent_ds', portal_status[portal]['inconsistent_ds']
        print '  inconsistent_res', portal_status[portal]['inconsistent_res']
    