'''
Created on Aug 10, 2015

@author: jumbrich
'''
from odpw.db.dbm import PostgressDBM
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.core import DCATConverter
from odpw.db.models import Dataset
from odpw.utils.dataset_converter import dict_to_dcat
from odpw.analysers.dbm_handlers import DCATDistributionInserter
from odpw.analysers.count_analysers import DCATDistributionCount


if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)




    id="www_data_gc_ca"
    did="fe3a1013-9877-40ac-8d90-61a78abcdc75"
    sn=1533

    d = dbm.getDataset(datasetID=did, snapshot=sn, portalID=id)
    
    for r in d.data['resources']:
        print r['url']
    #for d in  dcat:
    #    if '@type' in  d and 'http://www.w3.org/ns/dcat#Distribution' in d['@type']:
    #        print d 
    
    dc = DCATConverter(dbm.getPortal(portalID=id))
    d1= DCATDistributionCount()
    
    dc.analyse_Dataset(d)
    d1.analyse_Dataset(d)
    
    d1.done()
    
    print d1.getResult()
        