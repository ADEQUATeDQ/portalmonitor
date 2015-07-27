'''
Created on Jul 9, 2015

@author: jumbrich
'''

from odpw.db.models import Portal, PortalMetaData
from collections import defaultdict
from matplotlib.pyplot import plot


from odpw.analysers import AnalyseEngine, StatusAnalyserPMD, DatasetDistAnalyserPMD,\
    ResourceDistAnalyserPMD
from odpw.db.dbm import PostgressDBM




from odpw.timer import Timer
import json

from pprint import pprint 
import pandas as pd

if __name__ == '__main__':
    
    dbm= PostgressDBM(host="bandersnatch.ai.wu.ac.at")
    
    
    portals = dbm.getPortalMetaDatas(snapshot="2015-29")
    
    ae = AnalyseEngine()
    
    sa=StatusAnalyserPMD()
    da=DatasetDistAnalyserPMD()
    ra=ResourceDistAnalyserPMD()
    ae.add(sa)
    ae.add(da)
    ae.add(ra)
    
    ae.process_all(PortalMetaData.iter(portals))
    
    
    print sa.getResult()
    print sum(da.getResult()['total']),sum(da.getResult()['processed'])

    
