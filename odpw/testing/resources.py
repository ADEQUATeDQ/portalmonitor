'''
Created on Aug 17, 2015

@author: jumbrich
'''
from odpw.db.dbm import PostgressDBM
from odpw.analysers.core import DCATConverter
from odpw.analysers.count_analysers import DCATDistributionCount
from odpw.analysers import AnalyserSet, process_all
from odpw.db.models import Dataset, Resource
from odpw.analysers.dbm_handlers import DCATDistributionInserter

import urlnorm
from odpw.utils.util import progressIterator

if __name__ == '__main__':
    pass


    url='http://webrzs.stat.gov.rs/WebSite/userFiles/file/Nacionalni/GDPSerija1997_2009sajteng.xls'
    

    id='rs_ckan_net'
    sn=1533
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    
    #aset = AnalyserSet()
    #dc = aset.add(DCATConverter(dbm.getPortal(portalID=id)))
    #d1= aset.add(DCATDistributionCount(withDistinct=True))
    #aset.add(DCATDistributionInserter(dbm))
    
    #process_all(aset, Dataset.iter(dbm.getDatasets(snapshot=sn, portalID=id)))
    #print d1.getResult()
    
    delins={}
    for res in Resource.iter(dbm.getResources(snapshot=sn)):
        url = res.url
        
        url_new = url
        try:
            url_new = urlnorm.norm(url.strip())
            # props=util.head(url)
        except Exception as e:
            pass
    
        if url != url_new:
            r = res
            r.url=url_new
            delins[url] =r 
            
    t = len(delins)
    print t
    
    steps = t/10 if t>10 else 1
    for url, upR in progressIterator(delins.items(), t, steps):
        try:
            #delete resourece with url
            dbm.deleteResource(url, sn) 
            #and insert new Resource
            dbm.insertResource(upR)
        except Exception as e:
            pass
        
        