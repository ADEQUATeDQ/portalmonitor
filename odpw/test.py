from time import sleep
import requests
import logging
from odpw.db.dbm import PostgressDBM
from odpw.analysers import AnalyseEngine, PortalSoftwareDistAnalyser,\
    PortalCountryDistAnalyser, StatusCodeAnalyser
from odpw.reporting import PortalStatusReporter
from odpw.db.models import Portal
import pandas
from odpw.analysers import CountAnalser, getSoftware
__author__ = 'jumbrich'

from ConfigParser import SafeConfigParser

def toPortal(e):
    try:
        return Portal.fromResult(dict(e))
    except Exception as ex:
        return None
        

def scan(dbm):
    
    
    ae = AnalyseEngine(Portal.fromResult)
    
    
    softwareCount = CountAnalser(getSoftware)
    
    ae.add(softwareCount)
    
    #ae.add(PortalSoftwareDistAnalyser())
    #ae.add(PortalStatusReporter())
    #ae.add(PortalCountryDistAnalyser())
    
    ae.process_all( dbm.getPortals() )
    
    #for p in dbm.getPortals():
    #    pass
    
    ######
    
    
    #df=ae.getAnalyser(PortalSoftwareDistAnalyser).getDataFrame()
    #print df
    df=softwareCOunt.getDataFrame(columns=['software', 'count'])
    print df
    #ae.getAnalyser(PortalCountryDistAnalyser).getResult()
    
def getStatus(portal):
    return portal.status

def db(dbm):
    
    
    #results=[]
    #for res in  dbm.getSoftwareDist():
    #    results.append([res['software'], res['count']])
    #df = pandas.DataFrame(results, columns=['software', 'count'])
    #print df  
    
    results=[]  
    a = CountAnalser(getStatus)
    for res in dbm.getPortalStatusDist():
        a.add(res['status'], res['count'])

    df = pandas.DataFrame([[status,val['count'],val['label']] for status, val in a.getDist().items() ], columns=['status_prefix', 'count', 'label'])
    #print df  
    

if __name__ == '__main__':
    logging.basicConfig()
    dbm= PostgressDBM(host="bandersnatch.ai.wu.ac.at", port=5433)
    
    
    
    print 'Scan'    
    from timeit import Timer
    t = Timer(lambda: scan(dbm))
    r= t.repeat(number=1, repeat=1)
    print min(r), max(r)
    #print("%.2f usec/pass" % (1000000 * t.timeit(number=1000)/1000))

    print 'DB'
    t = Timer(lambda: db(dbm))
    r= t.repeat(number=1, repeat=1)
    print min(r), max(r)
    #print("%.2f usec/pass" % (1000000 * t.timeit(number=1000)/1000))
    
    