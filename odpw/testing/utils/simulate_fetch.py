'''
Created on Dec 10, 2015

@author: jumbrich
'''
from odpw.utils.fetch_stats import simulateFetching
from odpw.db.dbm import PostgressDBM

if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    
    
    ps = [
          #"http://data.ohouston.org/"
          #"http://healthdata.nj.gov"
    
          #"http://opendata.brussels.be"
         #"https://gavaobert.gavaciutat.cat",
         "http://oppnadata.se/"
         ]
    for i in ps:
        apiurl=i
        snapshot=1550
    
    
    
        p = dbm.getPortal(apiurl=apiurl)
        simulateFetching(dbm,{'Portal':p, 'snapshot':snapshot})