from odpw.analysers.sanity import FetchSanity, HeadSanity
from odpw.analysers import AnalyserSet, process_all

from odpw.reporting.sanity_reports import SanityReport, SanityReporter
from _collections import defaultdict

__author__ = 'jumbrich'

from odpw.db.models import Portal
from odpw.utils.util import getSnapshot, progressIterator
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
    
    pmds=[]
    
    aset= AnalyserSet()
    fs= aset.add(FetchSanity(dbm))
    rs= aset.add(HeadSanity(dbm))
    
    for portal in portals:
        
        snapshots=[]
        if not sn:
            for s in dbm.getSnapshots(portalID=portal.id):
                snapshots.append(s['snapshot'])
        else:
            snapshots.append(sn)
            
        for sn in snapshots:
            pmd = dbm.getPortalMetaData(portalID= portal.id, snapshot=sn)
            pmds.append(pmd)
        
    log.info("Sanity", pmds=len(pmds))
    t = len(pmds)
    s= t/10 if t>10 else 1
    process_all(aset, progressIterator(pmds,t,s))          

    sr= SanityReport([SanityReporter(fs),
                        SanityReporter(rs)])
    
    
    sr.clireport()
    sr.csvreport('.')
