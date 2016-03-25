from odpw.db.models import Portal, Dataset, Resource
from odpw.analysers import SAFEAnalyserSet, process_all, Analyser
from odpw.analysers.core import DCATConverter
from odpw.utils.dataset_converter import DCAT, DCT

__author__ = 'sebastian'

import argparse
import csv
import json
import os
import sys
import urllib
import pickle
from odpw.db.dbm import PostgressDBM
import urlnorm

import structlog
log =structlog.get_logger()


class CSVFilter(object):
    
    filters=set(['csv', 'csv', 'tsv', 'tsv','comma-separated-values','tab-separated-values'])
    
    @classmethod
    def filter(cls, formats):
        for t in cls.filters:
            for f in formats:
                if t in f.strip().lower():
                    return True
        return False
        



class DistributionExtractor(Analyser):
    
    formats={'csv':CSVFilter}
    
    @classmethod
    def filterFormats(cls):
        return cls.formats.keys()
    
    def __init__(self, urls, dbm, snapshot, portal, path, filter):
        self.urls = urls
        self.portal= portal
        self.path = path
        self.dbm= dbm
        self.snapshot = snapshot
        self.filter = filter
        
    def analyse_Dataset(self, dataset):
        for dcat_el in getattr(dataset,'dcat',[]):
            if str(DCAT.Distribution) in dcat_el.get('@type',[]):
                try:
                    
                    info={}
                    #try to normalise and validate url
                    url=self.getValue(dcat_el,DCAT.accessURL)
                    #print url
                    url_norm = urlnorm.norm(url.strip())
                    #print 'url_norm',url_norm
                    url_clean = urllib.quote(url_norm, safe="%/:=&?~#+!$,;'@()*[]")
                    #print 'url_clean',url_clean
                    
                    info['format'] = self.getValue(dcat_el, DCT['format'])
                    info['mediatype']=self.getValue(dcat_el,DCAT.mediaType)
                    info['name'] = self.getValue(dcat_el,DCT.title)
                    f=[ info['format'], info['mediatype'] ]
                    
                
                    if url_clean not in self.urls and len(url_clean) >0:
                        
                        #parse URL as Resource for lookups
                        tR =  Resource.newInstance( url=url, snapshot=self.snapshot)
                        R = self.dbm.getResource(tR) 
                        #print 'R',R
                        header=None
                        if R:
                            header={ 'size':R.size, 'mime':R.mime}
                            f.append(R.mime)
                        if DistributionExtractor.formats[ self.filter ].filter([info['format']]):
                            self.urls[url_clean] = {'portals': {}}
                            if self.portal.id not in self.urls[url_clean]['portals']:
                                self.urls[url_clean]['portals'][self.portal.id] = info
                            
                            if header:
                                self.urls[url_clean]['header']={'size':R.size,'mime':R.mime}
                except Exception as e:
                    print repr(e), e                    
      
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




def help():
    return "Extract URLs from datasets"
def name():
    return 'URLs'

def setupCLI(pa):
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    #pa.add_argument('-s','--software',choices=['CKAN', 'Socrata', 'OpenDataSoft'], dest='software')
    pa.add_argument('-o','--out',type=str, dest='out' , help="the out directory for the list of urls (and downloads)")
    pa.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")
    pa.add_argument('-f','--filter',type=str, dest='filter' , help="Filter by format (csv)", default='csv')
    pa.add_argument('--store',  action='store_true', default=False, help="store the files in the out directory")

def cli(args, dbm):
    if args.out is None:
        raise IOError("--out is not set")
    if args.snapshot is None:
        raise IOError("--snapshot not set")

    if not os.path.exists(args.out):
        os.makedirs(args.out)

    if args.url:
        p = dbm.getPortal(url=args.url)
        if not p:
            raise IOError(args.url + ' not found in DB')
        portals = [p]
    else:
        portals = [p for p in Portal.iter(dbm.getPortals())]

    if args.filter and not args.filter.lower() in DistributionExtractor.filterFormats():
        log.warn("Filter argument not known", filter=args.filter)
        return

    all_urls = {}
    for p in portals:
        try:
            
            extract_urls(all_urls, p, args.snapshot, dbm, args.out, args.store,args.filter)
        except Exception as e:
            print 'error in portal:', p.url
            print e

    fname='csv_urls_' + str(args.snapshot) + ("_"+portals[0].id) if len(portals)==1 else ''
    with open(os.path.join(args.out, fname+ '.pkl'), 'wb') as f:
        pickle.dump(all_urls, f)
        print 'Writing dict to ',f
    with open(os.path.join(args.out, fname+'.json'), 'w') as f:
        json.dump(all_urls, f)
        print 'Writing dict to ',f

def extract_urls(urls, portal, snapshot, dbm, out, store_files, filter):
    log.info("Extracting urls from ", portals=portal.id)
    path=None
    if store_files:
        path = os.path.join(out, portal.id)
        if not os.path.exists(path):
            os.makedirs(path)

    ae = SAFEAnalyserSet()
    ae.add(DCATConverter(portal))
    
    ae.add(DistributionExtractor(urls, dbm, snapshot, portal, path, filter))
    
    iter = Dataset.iter(dbm.getDatasetsAsStream(portal.id, snapshot=snapshot))
    process_all( ae, iter)
        
    


def store_file(url, filename):
    testfile = urllib.URLopener()
    testfile.retrieve(url, filename)


def valid_filename(s):
    return "".join(x for x in s if x.isalnum() or x == '.')


#def extract_ckan_urls(urls, portal, snapshot, dbm, out, store_files):
#    
#
#    for dataset in Dataset.iter(dbm.getDatasetsAsStream(portal.id, snapshot=snapshot)):
#        data = dataset.data
#        if data is not None and 'resources' in data:
 #           for res in data.get("resources"):
 #               format = res.get("format", '').strip().lower()
 #               if format in csv_related_formats:
 #                   url = None
 #                   try:
 #                       url = res.get("url").strip()
 #                       name = res.get('name', url)
 #                       # store metadata
 #                       if url not in urls:
 #                           urls[url] = {'portal': {}}
 #                       urls[url]['title'] = name
 #                       urls[url]['format'] = format
 #                       if portal.id not in urls[url]['portal']:
 #                           urls[url]['portal'][portal.id] = []
 #                       urls[url]['portal'][portal.id].append(data.get('name'))
#
#                        if store_files:
#                            filename = os.path.join(path, valid_filename(url))
#                            if url not in urls:
#                                store_file(url, filename)
#                                urls[url]['file'] = filename
#                    except Exception as e:
#                        print 'error loading url:', url
#                        print e
