from time import sleep
import requests
import logging
from odpw.db.dbm import PostgressDBM
__author__ = 'jumbrich'

from ConfigParser import SafeConfigParser



from multiprocessing import Process, Pool

def fetch(i):
    print "fetch",i
    sleep(10)

if __name__ == '__main__':
    logging.basicConfig()
    p= PostgressDBM(host="bandersnatch.ai.wu.ac.at", port=5433)
    
    
    
    
