'''
Created on Aug 10, 2015

@author: jumbrich
'''
from odpw.db.dbm import PostgressDBM
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.core import DCATConverter
from odpw.db.models import Dataset


if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)

    id='data_wu_ac_at'
    sn=1532

    Portal = dbm.getPortal(portalID=id)
    iter = Dataset.iter(dbm.getDatasets(portalID=Portal.id, snapshot=sn))
    
    aset = AnalyserSet()
    
    aset.add(DCATConverter(Portal))
    
    process_all(aset, iter)