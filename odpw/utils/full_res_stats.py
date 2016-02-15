# -*- coding: utf-8 -*-

import odpw.utils.util as util
from odpw.analysers import AnalyseEngine, process_all, SAFEAnalyserSet
from odpw.analysers import AnalyserSet
from odpw.analysers.dataset_analysers import DatasetChangeCountAnalyser,\
    ResourceChangeInfoAnalyser
import os
import pickle
from odpw.analysers.core import DCATConverter

__author__ = 'jumbrich'

from odpw.utils.util import progressIterator, getSnapshot

from odpw.db.models import Portal, Dataset, Resource

import structlog
log =structlog.get_logger()



def help():
    return "Get a complete view of resources and their change information"

def name():
    return 'ResChangeInfo'

def setupCLI(pa):
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")
    pa.add_argument('-o','--out',type=str, dest='output' , help="outputdir")
    
    
def cli(args,dbm):
    sn = getSnapshot(args)
    if not sn:
        return
    
    portals=[]
    if args.url:
        p = dbm.getPortal(apiurl=args.url)
        if p:
            portals.append(p)
    else:
        for p in Portal.iter(dbm.getPortals()):
            portals.append(p)
    
    
    filename=os.path.join(args.output,"res_change_info.csv")
    with open(os.path.abspath(filename),"w") as f:
        res=[   'url',
                            'software',
                            'portal.id',
                            'local',
                            'http_lm',
                            'http_etag',
                            'meta_last_modified',
                            'meta_webstore_url',
                            'meta_webstore_last_updated',
                            'update_frequeny']
        f.write(",".join(res)+"\n")
    res={}
    
    resources={}
    #iter=Resource.iter(dbm.getResourcesWithHeader())
    #for R in iter:
    #    if R.url not in resources:
    #        resources[R.url]=R
    
    for p in portals:
        total=dbm.countDatasets(portalID=p.id, snapshot=sn)
        
        
        
        
        steps=total/10
        if steps ==0:
            steps=1
        
         
        ae= AnalyserSet()
        ae.add(DCATConverter(p))
        ae.add(ResourceChangeInfoAnalyser(filename,p,dbm, resources))
        
        iter = Dataset.iter(dbm.getDatasets(portalID=p.id, snapshot=sn))
        process_all(ae, progressIterator(iter, total, steps, label=p.id))
    
    
    
    
