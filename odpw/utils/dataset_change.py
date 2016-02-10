

import odpw.utils.util as util
from odpw.analysers import AnalyseEngine, process_all, SAFEAnalyserSet
from odpw.analysers import AnalyserSet
from odpw.analysers.dataset_analysers import DatasetChangeCountAnalyser
import os
import pickle

__author__ = 'jumbrich'

from odpw.utils.util import progressIterator

from odpw.db.models import Portal, Dataset

import structlog
log =structlog.get_logger()



def help():
    return "Simulate a fetch run"

def name():
    return 'DSChange'

def setupCLI(pa):
    
    pa.add_argument("-f","--from",  help='what snapshot is it', dest='snfrom')
    pa.add_argument("-t","--to",  help='what snapshot is it', dest='snto')
    pa.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")
    pa.add_argument('-o','--out',type=str, dest='output' , help="outputdir")
    
    
def cli(args,dbm):
    portals=[]
    if args.url:
        p = dbm.getPortal(apiurl=args.url)
        if p:
            portals.append(p)
    else:
        for p in Portal.iter(dbm.getPortals()):
            portals.append(p)
    
    
    snfrom=args.snfrom
    snto=args.snto
    
    res={}
    for p in portals:
        
        iter = Dataset.iter(dbm.getDatasets(portalID=p.id, snapshot=snfrom))
        
        datasetsfrom={}
        for D in iter:
            datasetsfrom[D.id]=D
        
        total=dbm.countDatasets(portalID=p.id, snapshot=snto)
        
        steps=total/10
        if steps ==0:
            steps=1
         
        ae= AnalyserSet()
        cha= ae.add(DatasetChangeCountAnalyser(datasetsfrom))
        iter = Dataset.iter(dbm.getDatasets(portalID=p.id, snapshot=snto))
        process_all(ae, progressIterator(iter, total, steps, label=p.id))
    
        res[p.id]=cha.getResult()
    
    
    with open(os.path.abspath(os.path.join(args.output,'changes.pkl')), 'wb') as output_file:
        print 'Pickling to',output_file
        pickle.dump( res, output_file )
        
    
    for k,v in res.items():
        print k
        for c,cnt in v.items():
            print '  ',c,cnt
    
    
    
