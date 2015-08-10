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
    main()