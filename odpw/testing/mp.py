'''
Created on Aug 3, 2015

@author: jumbrich
'''
import time
import numpy as np
import time
import multiprocessing
import signal
from random import randint
from multiprocessing import Pool
import functools
from odpw.utils.util import TimeoutError
import sys






def function_that_takes_a_long_time(t, timeout):
        # ... long, parallel calculation ...
        i=randint(3,9)
        print t,"has",i
        start=time.time()
        
        for a in range(i):
            time.sleep(1)
            now = time.time()
            if now-start>timeout:
                raise TimeoutError("Timeout of "+str(t)+" and "+str(timeout)+" seconds", timeout)
            yield a

def fetching(t):
    try:
        for i in  function_that_takes_a_long_time(t,5):
            continue
    except TimeoutError, (e):
        print e
        print dir(e)
        print e.timeout


def main():

    pnum = 10    
     
    procs = []
    p = Pool(2)
    a = range(pnum)
    print a
    p.map(fetching, a)
        

if __name__ == "__main__":
    #main()
    
    import datetime
    print datetime.datetime(2014, 6, 1).isoformat()
    print datetime.date(2014, 6, 1).isoformat()
    
    sys.exit(0)
    
    from pandas import DataFrame
    from pandas import merge
    left = DataFrame({'key1': ['K0', 'K0', 'K1', 'K2'],
                      'key2': ['K0', 'K1', 'K0', 'K1'],
                      'A': ['A0', 'A1', 'A2', 'A3'],
                      'B': ['B0', 'B1', 'B2', 'B3']})
   

    print left
    right = DataFrame({'key1': ['K0', 'K1', 'K1', 'K2'],
                       'key2': ['K0', 'K0', 'K0', 'K0'],
                       'C': ['C0', 'C1', 'C2', 'C3'],
                       'D': ['D0', 'D1', 'D2', 'D3']})
    

    result = left.join(right, on=['key1', 'key2'])
    print result