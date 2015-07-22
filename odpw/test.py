from time import sleep
import requests
import logging
from odpw.db.dbm import PostgressDBM
from odpw.quality.analysers import AnalyseEngine, PortalSoftwareDistAnalyser,\
    PortalCountryDistAnalyser
from odpw.reports import PortalStatusReporter
from odpw.db.models import Portal
__author__ = 'jumbrich'

from ConfigParser import SafeConfigParser


def scan(dbm):
    ae = AnalyseEngine()
    
    ae.add(PortalSoftwareDistAnalyser())
    #ae.add(PortalStatusReporter())
    #ae.add(PortalCountryDistAnalyser())
    
    ae.process_all( Portal.iter(dbm.getPortals()) )
    
    ######
    
    
    ae.getAnalyser(PortalSoftwareDistAnalyser).getResult()
    #ae.getAnalyser(PortalStatusReporter).getResult()
    #ae.getAnalyser(PortalCountryDistAnalyser).getResult()
    
def db(dbm):
    
    print dbm.getSoftwareDist()
    

if __name__ == '__main__':
    logging.basicConfig()
    dbm= PostgressDBM(host="bandersnatch.ai.wu.ac.at", port=5433)
    
    
    
    
    from timeit import Timer
    t = Timer(lambda: scan(dbm))
    r= t.repeat(number=1, repeat=10)
    print min(r), max(r)
    print("%.2f usec/pass" % (1000000 * t.timeit(number=1000)/1000))

    t = Timer(lambda: db(dbm))
    r= t.repeat(number=1, repeat=10)
    print min(r), max(r)
    print("%.2f usec/pass" % (1000000 * t.timeit(number=1000)/1000))
    
    