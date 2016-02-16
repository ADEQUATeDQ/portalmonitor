'''
Created on Jan 22, 2016

@author: jumbrich
'''
from odpw.db.dbm import PostgressDBM
from odpw.utils.head import head

if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    
    
    url='http://www.midyorks.nhs.uk/download.cfm?doc=docm93jijm4n2322.pdf&ver=2409'
    #url='http://www.midyorks.nhs.uk/download.cfm?doc=docm93jijm4n2269.pdf&ver=2356'
    r = dbm.getResourceByURL(url=url, snapshot=1603)
    
    head(dbm, 1603, None, r)
    
    
    