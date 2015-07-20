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


from stats import simulateFetch
def name():
    return 'Status'

def setupCLI(pa):
    pass

def cli(args,dbm):
    
    portal_status={}
    for res in dbm.getPortals():
        portal = Portal.fromResult(dict(res))
        
        
        stats={'missing_pmds':[], 'OK':[], 'ds_missmatch':[]}
        print 'Analysing', portal.id
        
        
        for res in dbm.datasetsPerSnapshot(portalID=portal.id):
            pmd = dbm.getPortalMetaData(snapshot=res['snapshot'], portalID=portal.id)
            
            if not pmd or not pmd.fetch_stats or 'datasets' not in pmd.fetch_stats: 
                stats['missing_pmds'].append( res['snapshot'])
                
                total=0
                for count in dbm.countDatasets(portalID=portal.id, snapshot=res['snapshot']):
                    total=count[0]
        
                #if total< 5000:
                    
                #    simulateFetch(portal, dbm, res['snapshot'])
                
            elif res['datasets'] !=  pmd.fetch_stats['datasets']:
                stats['ds_missmatch'].append( res['snapshot'])
                total=0
                for count in dbm.countDatasets(portalID=portal.id, snapshot=res['snapshot']):
                    total=count[0]
        
                #if total< 10000:
                #    simulateFetch(portal, dbm, res['snapshot'])
            else:
                stats['OK'].append( res['snapshot'])
                
                
        portal_status[portal.id]=stats
            #dataset = Dataset.fromResult(dict(dsres))
            #stats[dataset.snapshot]+=1
        #print stats
    
    ok=0 
    missing_pmds=0 
    ds_missmatch=0
    
    for portal in portal_status.keys():
        if len(portal_status[portal]['missing_pmds']) >0 or len(portal_status[portal]['ds_missmatch'])>0:
            print "--", portal
            print "  OK", portal_status[portal]['OK']
            print "  missing_pmds", portal_status[portal]['missing_pmds']
            print "  ds_missmatch", portal_status[portal]['ds_missmatch']
        ok+=len(portal_status[portal]['OK'])
        missing_pmds+=len(portal_status[portal]['missing_pmds'])
        ds_missmatch+=len(portal_status[portal]['ds_missmatch'])
    
    print 'Overall', ok+missing_pmds+ds_missmatch
    print 'ds_missmatch', ds_missmatch
    print 'missing_pmds', missing_pmds
    print 'ok', ok
