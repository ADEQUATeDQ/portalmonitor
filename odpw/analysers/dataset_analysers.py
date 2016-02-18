# -*- coding: utf-8 -*-
'''
Created on Feb 10, 2016

@author: jumbrich
'''
from freshness import json_compare
from odpw.analysers.core import ElementCountAnalyser
from odpw.analysers import Analyser
from odpw.utils.dataset_converter import DCAT
import tldextract
from odpw.utils.util import ErrorHandler

import structlog
import urlnorm
log =structlog.get_logger()

class DatasetChangeCountAnalyser(ElementCountAnalyser):
    def __init__(self, datasets):
        super(DatasetChangeCountAnalyser, self).__init__()
        self.datasets=datasets
        
    def analyse_Dataset(self, dataset):
        if dataset.data and dataset.id in self.datasets:
            diffs = json_compare.jsondiff(self.datasets[dataset.id].data, dataset.data)
            for mode, selector, changes in diffs:
                try:
                    k= mode+'_'+"_".join([str(s) for s in selector])
                    self.add(k)
                except Exception as e:
                    print dataset.id, dataset.portal_id, e
            
            
class ResourceChangeInfoAnalyser(Analyser):
    
    def __init__(self, outfile, Portal, dbm, resources):
        super(ResourceChangeInfoAnalyser, self).__init__()
        self.out=open(outfile, "a")
        self.portal=Portal
        self.dbm=dbm
        self.resources=resources
        self.path='.'.join(tldextract.extract(Portal.apiurl)[:2]).lower()
        
    
    def analyse_Dataset(self, dataset):
        #get the distribution url from dcat
        for dcat_el in getattr(dataset,'dcat',[]):
            if str(DCAT.Distribution) in dcat_el.get('@type',[]):
                url=None
                
                durl = dcat_el.get(str(DCAT.downloadURL),[])
                for du in durl:
                    url = du.get('@value',None)
                    if url: 
                        break
                    url = du.get('@id',None)
                    
                if not url:
                    aurl=dcat_el.get(str(DCAT.accessURL),[])
                    for au in aurl: 
                        url = au.get('@value',None)
                        if url: 
                            break
                        url = au.get('@id',None)
                if url:
                    #yeah we have a url
                    
                    """ url, 
                        software, 
                        portal_id, 
                        local
                        http_lm
                        http_etag
                        meta_last_modified
                        meta_webstore_last_updated
                        meta_webstore_url
                        
                    """
                    local=False
                    http_lm=False
                    http_etag=False
                    
                    meta_webstore_url='mis'
                    meta_webstore_last_updated='mis'
                    meta_last_modified='mis'
                    update_frequeny='mis'
                    
                    
                    #lets figure out if this is local or external
                    #1) socrate and opendatasoft are local
                    #2) CKAN 
                    #    2a) url matches
                    #    2b) webstore
                    #    2b) url = upload
                    
                    if self.portal.software == 'CKAN':
                        #CKAN case
                        #1) check url match
                        
                        try:
                            path='.'.join(tldextract.extract(url)[:2]).lower()
                            local = path==self.path
                        except Exception as e:
                            pass
                        
                        #2) check for metadata keys
                        if 'resources' in dataset.data:
                            for r in dataset.data['resources']:
                                try:
                                    r_url = urlnorm.norm(r['url'].strip())
                                    
                                    if url==r['url']:
                                        
                                        if not local and 'url_type' in r:
                                            local= r['url_type']=='upload'
                                        if 'last_modified' in r :
                                            meta_last_modified='empty' if r['last_modified'] is None or r['last_modified'] == "" else 'value'
                                        if 'webstore_url' in r :
                                            meta_webstore_url='empty' if r['webstore_url'] is None or r['webstore_url'] == "" else 'value'
                                        if 'webstore_last_updated' in r :
                                            meta_webstore_last_updated='empty' if r['webstore_last_updated'] is None or r['webstore_last_updated'] == "" else 'value'
                                except Exception as e:
                                    pass
                        if 'extras' in dataset.data and 'update_frequency' in dataset.data['extras']:
                            update_frequeny=dataset.data['extras']['update_frequency']
                        
                    elif self.portal.software == 'Socrata':
                        local=True
                        if 'view' in dataset.data and 'publicationDate' in dataset.data['view'] and 'rowsUpdatedAt' in dataset.data['view']:
                            meta_last_modified= "same" if dataset.data['view']['publicationDate']== dataset.data['view']['rowsUpdatedAt'] else 'value'
                        #rowsUpdatedAt
                        #viewLastModified
                    else:
                        local=True
                    
                    ## http last-modified and etag
                    if url in self.resources:
                        header= self.resources[url].header
                        for k in header: 
                            if k.lower().strip() == 'last-modified':
                                http_lm =True
                            if k.lower().strip() == 'etag':
                                http_etag =True
                    
                    
                    res=[   url.encode('utf8'),
                            self.portal.software.encode('utf8'),
                            self.portal.id.encode('utf8'),
                            str(local).encode('utf8'),
                            str(http_lm).encode('utf8'),
                            str(http_etag).encode('utf8'),
                            str(meta_last_modified).encode('utf8'),
                            str(meta_webstore_url).encode('utf8'),
                            str(meta_webstore_last_updated).encode('utf8'),
                            str(update_frequeny).encode('utf8')]
                    try:
                        self.out.write(",".join(res)+"\n")
                    except Exception as e: 
                        ErrorHandler.handleError(log, "Writting csv info", exception =e, url=url, pid=self.portal.id)
                    
        
    
    def done(self):
        self.out.close()