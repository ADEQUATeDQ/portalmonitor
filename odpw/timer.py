__author__ = 'jumbrich'


import time
#import numpy
import faststat

class Timer(object):

    measures={}

    def __init__(self, verbose=False, key=None):
        self.verbose = verbose
        self.key=key

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        self.msecs = self.secs * 1000  # millisecs
        if self.verbose:
            print '(%s) elapsed time: %f ms' % (self.key,self.msecs)
        if self.key:
            if self.key not in Timer.measures:
                Timer.measures[self.key]=faststat.Stats()
            Timer.measures[self.key].add(self.msecs)

    @classmethod
    def printStats(cls):
        import pprint
        pprint.pprint(cls.getStats())


    @classmethod
    def getStats(cls):
        stats={}
        for m in Timer.measures:
            stats[m]={'avg':Timer.measures[m].mean, 'calls':Timer.measures[m].n, 'min':Timer.measures[m].min, 'max':Timer.measures[m].max}

