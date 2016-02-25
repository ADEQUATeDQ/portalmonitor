'''
Created on Dec 10, 2015

@author: jumbrich
'''

from odpw.db.dbm import PostgressDBM
from odpw.reporting.info_reports import portalinfo

from pprint import pprint
if __name__ == '__main__':
    
    
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    
    sn=1550
    portalID='data_wu_ac_at'
    report = portalinfo(dbm, sn, portalID)
    
    pprint(report.uireport())