# -*- coding: utf-8 -*-

import odpw.utils.util as util
from odpw.analysers import AnalyseEngine, process_all, SAFEAnalyserSet, Analyser
from odpw.analysers import AnalyserSet
from odpw.analysers.dataset_analysers import DatasetChangeCountAnalyser,\
    ResourceChangeInfoAnalyser
import os
import pickle
from odpw.analysers.core import DCATConverter
from odpw.utils.dataset_converter import DCAT, DCT

__author__ = 'jumbrich'

from odpw.utils.util import progressIterator, getSnapshot

from odpw.db.models import Portal, Dataset, Resource

import structlog
log =structlog.get_logger()



class SizeEstimator(Analyser):




    def __init__(self, resources):
        super(SizeEstimator,self).__init__()
        self.res=resources

        self.filter=[
            'csv', 'xml', 'json', 'csv-semicolon delimited', 'rdf', 'rss','xls','xlsx','tsv','comma-separated-values','tab-separated-values'
        ]

        self.results={'per_res':{'hsize':0, 'msize':0, 'count':0, 'total':0,'hempty':0}}


    def getValue(self,dcat_el,key):
        key=str(key)
        value=''
        for f in dcat_el.get(key,[]):
            if '@value' in f:
                v = f.get('@value','')
                value = v.strip()
            elif '@id' in f:
                v = f.get('@id','')
                value=v.strip()
        return value

    def analyse_Dataset(self, dataset):
        for dcat_el in getattr(dataset,'dcat',[]):
            if str(DCAT.Distribution) in dcat_el.get('@type',[]):
                try:
                    format = self.getValue(dcat_el, DCT['format']).replace(".","")
                    media=self.getValue(dcat_el,DCAT.mediaType)
                    formats=[ format.lower().strip(), media.lower().strip() ]

                    sizeByte=0
                    try:
                        sizeByte=int(self.getValue(dcat_el,DCAT.byteSize))
                    except Exception:
                        sizeByte=0
                        pass


                    url=self.getValue(dcat_el,DCAT.accessURL)
                    R=None
                    hsize=None
                    hmime=None
                    if url in self.res:
                        R= self.res[url]
                        hsize=R.size
                        hmime=R.mime
                        if R.mime is not None:
                            formats.append(R.mime.lower().strip())


                    if sizeByte is None:
                        sizeByte=0
                    if hsize is None:
                        hsize=0
                    self.results['per_res']['total']+=1
                    if any([f in formats for f in self.filter]):
                        self.results['per_res']['hsize']+=hsize

                        self.results['per_res']['msize']+=sizeByte
                        self.results['per_res']['count']+=1
                        if hsize==0:
                            self.results['per_res']['hempty']+=1


                    for f in self.filter:
                        if f in formats:
                            stats= self.results.setdefault(f,{'hsize':0, 'msize':0, 'count':0, 'total':0,'hempty':0})
                            stats['hsize']+=hsize
                            stats['msize']+=sizeByte
                            stats['count']+=1
                            if hsize==0:
                                stats['hempty']+=1



                except Exception as e:
                    print repr(e), e


    def getResult(self):
        return self.results




def help():
    return "Estimate file size for various formats"

def name():
    return 'Size'

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
    


    resources={}
    print "querying Resources"
    iter=Resource.iter(dbm.getResourcesWithHeader(since=1530))
    for R in iter:
        if R.url not in resources:
            resources[R.url]=R


    total=dbm.countDatasets( snapshot=sn)
    print 'found', total, 'datasets'

    steps=total/100
    if steps ==0:
        steps=1


    ae= AnalyserSet()
    ae.add(DCATConverter(p))
    se=ae.add(SizeEstimator(resources))



    iter = Dataset.iter(dbm.getDatasetsAsStream( snapshot=sn))
    process_all(ae, progressIterator(iter, total, steps, label=p.id))
    print se.getResult()

    with open('data.pkl', 'wb') as f:
        pickle.dumps(se.getResult(), f)





    import sys
    sys.exit(0)

    res={}
    resources={}
    for p in portals:
        print "querying Resources"
        iter=Resource.iter(dbm.getResourcesWithHeader(portal_id=p.id, since=1610))
        print "loading Resources"
        for R in iter:
            if R.url not in resources:
                resources[R.url]=R
        print 'found', len(resources),'with header'
        total=dbm.countDatasets(portalID=p.id, snapshot=sn)
        

        steps=total/10
        if steps ==0:
            steps=1
        
         
        ae= AnalyserSet()
        ae.add(DCATConverter(p))
        se=ae.add(SizeEstimator(resources))


        
        iter = Dataset.iter(dbm.getDatasets(portalID=p.id, snapshot=sn))
        process_all(ae, progressIterator(iter, total, steps, label=p.id))

        print se.getResult()
    
    
    
