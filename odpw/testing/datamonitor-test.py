'''
Created on Dec 3, 2015

@author: jumbrich
'''
from odpw.db.dbm import DMManager

if __name__ == '__main__':
    
    dm_dbm = DMManager(db='datamonitor', host="datamonitor-data.ai.wu.ac.at", port=5432, password='d4tamonitor', user='datamonitor')
    
    uri="http://www.pegelonline.wsv.de/gast/pegelinformationen"
    print dm_dbm.getLatestURLInfo(uri)