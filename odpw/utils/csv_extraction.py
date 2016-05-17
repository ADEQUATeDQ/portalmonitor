from pybloom import ScalableBloomFilter

from odpw.db.models import Portal, Dataset, Resource
from odpw.analysers import SAFEAnalyserSet, process_all, Analyser
from odpw.analysers.core import DCATConverter
from odpw.utils.dataset_converter import DCAT, DCT

__author__ = 'sebastian'

import copy

import argparse
import csv
import json
import os
import sys
import urllib
import pickle
from odpw.db.dbm import PostgressDBM
import urlnorm

from urlparse import urlparse

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
    
    @classmethod
    def filterFormats(cls):
        return cls.formats.keys()
    
    def __init__(self, urls, dbm,  portal, stats):
        self.urls = urls
        self.portal= portal
        self.dbm= dbm
        self.stats=stats

        self.filters=set(['csv', 'tsv','comma-separated-values','tab-separated-values'])

        tmp={  'valid_http':0
               ,'bloom':ScalableBloomFilter(error_rate=0.00001)
               ,'csvformat':0
               ,'csvinurl':0
               ,'csv':0
               ,'count':0
               ,'distinct':0
               }

        if 'all' not in self.stats:
            self.stats['all']={
               'total':copy.deepcopy(tmp)
             , 'distinct':copy.deepcopy(tmp)

            }

        if portal.id not in self.stats:
            self.stats[portal.id]={
               'total':copy.deepcopy(tmp)
             , 'distinct':copy.deepcopy(tmp)

            }
        if portal.software not in self.stats:
            self.stats[portal.software]={
               'total':copy.deepcopy(tmp)
             , 'distinct':copy.deepcopy(tmp)

            }


        
    def analyse_Dataset(self, dataset):

        for dcat_el in getattr(dataset,'dcat',[]):
            if str(DCAT.Distribution) in dcat_el.get('@type',[]):

                url=self.getValue(dcat_el,DCAT.accessURL)

                for k in ['all',self.portal.id,self.portal.software ]:
                    self.stats[k]['total']['count']+=1

                url_norm=None
                try:
                    #print url
                    url_norm = urlnorm.norm(url.strip().lower())
                    #print 'url_norm',url_norm

                    url_clean = urllib.quote(url_norm, safe="%/:=&?~#+!$,;'@()*[]")
                    #print 'url_clean',url_clean

                    #try to normalise and validate url

                except Exception as e:
                    #not valid url
                    continue

                info={}

                info['format'] = self.getValue(dcat_el, DCT['format']).lower()
                info['mediatype']=self.getValue(dcat_el,DCAT.mediaType).lower()
                #info['name'] = self.getValue(dcat_el,DCT.title)


                qu= urlparse(url).query
                if qu:
                    qu = qu.lower()

                #is this a CSV file?
                csv_format=  any([ fi in info['format'] or fi in info['mediatype'] for fi in self.filters])
                csv_in_url= any( [ fi in url_norm for fi in ['.csv', '.tsv']] + [ fi in qu for fi in ['csv', 'tsv']])


                for k in ['all', self.portal.id, self.portal.software ]:
                    self.stats[k]['total']['valid_http']+=1

                    if csv_format:
                        self.stats[k]['total']['csvformat']+=1
                    if csv_in_url:
                        self.stats[k]['total']['csvinurl']+=1
                    if csv_format and csv_in_url:
                        self.stats[k]['total']['csv']+=1

                    if self.stats[k]['total']['bloom'] is not None and url_norm not in self.stats[k]['total']['bloom']:
                        self.stats[k]['distinct']['count']+=1
                        self.stats[k]['distinct']['distinct']+=1
                        self.stats[k]['total']['bloom'].add(url_norm)

                        self.stats[k]['distinct']['valid_http']+=1
                        if csv_format:
                            self.stats[k]['distinct']['csvformat']+=1
                        if csv_in_url:
                            self.stats[k]['distinct']['csvinurl']+=1
                        if csv_format and csv_in_url:
                            self.stats[k]['distinct']['csv']+=1


                if csv_format or csv_in_url:

                    if url_clean in self.urls:
                        if self.portal.id in self.urls[url_clean]:
                            self.urls[url_clean][self.portal.id][dataset.id]= info
                        else:
                            self.urls[url_clean][self.portal.id]={ dataset.id: info }

                    else:
                        self.urls[url_clean]={
                            self.portal.id: { dataset.id: info, 'software':self.portal.software, 'iso3':self.portal.iso3 }
                        }

      
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
    return "Extract CSVs from datasets"
def name():
    return 'CSVs'

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

    all_urls = {}

    stats={}
    for p in portals:
        try:

            ae = SAFEAnalyserSet()
            ae.add(DCATConverter(p))


            ae.add(DistributionExtractor(all_urls, dbm, p,stats))

            iter = Dataset.iter(dbm.getDatasetsAsStream(p.id, snapshot=args.snapshot))
            process_all( ae, iter)

        except Exception as e:
            print 'error in portal:', p.url
            print e
            import sys, traceback
            traceback.print_exc(file=sys.stdout)

    import pprint
    pprint.pprint(stats)

    for k in stats:
        del stats[k]['total']['bloom']
        del stats[k]['distinct']['bloom']


    #pprint.pprint(all_urls)

    fname='csv_urls_' + str(args.snapshot)
    if len(portals)==1:
        fname = fname+ ("_"+portals[0].id)

    with open(os.path.join(args.out, fname+ '.pkl'), 'wb') as f:
        pickle.dump(all_urls, f)
        print 'Writing dict to ',f
    with open(os.path.join(args.out, fname+'.json'), 'w') as f:
        json.dump(all_urls, f)
        print 'Writing dict to ',f

    with open(os.path.join(args.out, fname+'_stats.json'), 'w') as f:
        json.dump(stats, f)
        print 'Writing dict to ',f

