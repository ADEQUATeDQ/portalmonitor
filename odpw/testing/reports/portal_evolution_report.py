'''
Created on Dec 19, 2015

@author: jumbrich
'''
from odpw.reporting.evolution_reports import portalevolution
from odpw.db.dbm import PostgressDBM
from pprint import pprint

if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    
    sn=1550
    portalID='data_wu_ac_at'
    
    report = portalevolution(dbm, sn, portalID)
    
    pprint(report.uireport())

