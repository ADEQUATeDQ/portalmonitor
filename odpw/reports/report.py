'''
Created on Jul 9, 2015

@author: jumbrich
'''

from odpw.db.models import Portal
from collections import defaultdict
from matplotlib.pyplot import plot
from csvkit.table import Table

from odpw.quality.analysers import AnalyseEngine, PortalSoftwareDistAnalyser,\
    PortalStatusAnalyser
from odpw.db.dbm import PostgressDBM
from odpw.reports import PortalStatusReporter



from odpw.timer import Timer

if __name__ == '__main__':
    
    dbm= PostgressDBM(host="bandersnatch.ai.wu.ac.at")
    ae = AnalyseEngine()
    
    ae.add(PortalSoftwareDistAnalyser())
    ae.add(PortalStatusReporter())
    
    portals = dbm.getPortals()
    
    ae.process_all(portals)
    
    sda = ae.getAnalyser(PortalSoftwareDistAnalyser)
    psa = ae.getAnalyser(PortalStatusReporter)
    
    print sda.getDataFrame()
    print psa.getDataFrame().dtypes
    
    #psa.plot()
    
    Timer.printStats()
    


