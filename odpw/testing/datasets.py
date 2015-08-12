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

    id='pod_opendatasoft_com'
    sn=1533

    portals = dbm.getPortals(software='Socrata')
    for p in portals:
        iter = Dataset.iter(dbm.getDatasets(portalID=p.id, snapshot=sn))
    
        aset = AnalyserSet()
    
        aset.add(DCATConverter(p))
        process_all(aset, iter)
        