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
    #pass


    #url='http://webrzs.stat.gov.rs/WebSite/userFiles/file/Nacionalni/GDPSerija1997_2009sajteng.xls'
    

    id='data_wu_ac_at'
    sn=1536
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    
    pmd = dbm.getPortalMetaData(portalID=id, snapshot=sn)
    aset = AnalyserSet()
    dc = aset.add(DCATConverter(dbm.getPortal(portalID=id)))
    d1= aset.add(DCATDistributionCount(withDistinct=True))
    di= aset.add(DCATDistributionInserter(dbm))
    
    process_all(aset, Dataset.iter(dbm.getDatasets(snapshot=sn)))
    aset.update(pmd)

    
    delins={}
    c=0
    
    a=0
    er=0
   #============================================================================
   #  for url in set(di.getResult()):
   #      
   #      try:
   #          tR =  Resource.newInstance(url=url, snapshot=sn)
   #          R = dbm.getResourceByURL(tR.url, sn)
   #          if R is None:
   #              
   #              R = dbm.getResource(tR)
   #              if not R:
   #                  print "MISSING"
   #                  #tR.updateOrigin(pid=id, did=dataset.id)
   #                  dbm.insertResource(tR)
   #                  
   #                  R = dbm.getResource(tR)
   #                  if not R:
   #                      print "MISSING"
   #              else:
   #                  print "EXISTIS", R.url
   #                  print "EXISTIS", url
   #              break;
   #          # props=util.head(url)
   #          else:
   #              if 'rs_ckan_net' not in R.origin: 
   #                  print "NO ORIGIN for ", R.url
   #              a+=1
   #      except Exception as e:
   #          er+=1
   #          pass
   # 
   #  print a, er
   #============================================================================
        
        
    
    for res in Resource.iter(dbm.getResources(snapshot=sn, portalID=id)):
        c+=1
        if len(res.origin.keys())>1:
            print res.origin
    print c
    
    
        
        