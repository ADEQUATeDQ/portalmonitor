'''
Created on Jul 9, 2015

@author: jumbrich
'''

from odpw.db.models import Portal, PortalMetaData
from collections import defaultdict
from matplotlib.pyplot import plot
from csvkit.table import Table

from odpw.quality.analysers import AnalyseEngine, PortalSoftwareDistAnalyser,\
    PortalStatusAnalyser, PortalMetaDataStatusAnalyser,\
    PortalMetaDataFetchStatsAnalyser
from odpw.db.dbm import PostgressDBM
from odpw.reports import PortalStatusReporter, PortalMetaDataStatusReporter

import pandas as pd

from odpw.timer import Timer
import vincent
import random
import json

from pprint import pprint 

if __name__ == '__main__':
    
    dbm= PostgressDBM(host="bandersnatch.ai.wu.ac.at")
    ae = AnalyseEngine()
    ae.add(PortalMetaDataStatusReporter())
    
    portals = dbm.getPortalMetaDatas()
    ae.process_all(PortalMetaData.iter(portals))
    
    pmdfs = ae.getAnalyser(PortalMetaDataFetchStatsAnalyser)
    print pmdfs.getResult()
    
    data = ae.getAnalyser(PortalMetaDataStatusReporter).getResult()
    pprint(data)
    
    vd= ae.getAnalyser(PortalMetaDataStatusReporter).getVegaData()
    print json.dumps(vd)
    Timer.printStats()
    


