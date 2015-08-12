__author__ = 'jumbrich'


import time
#import numpy
import faststat
import pprint

class Timer(object):

    measures={}

    def __init__(self, verbose=False, key=None):
        self.verbose = verbose
        self.key=key

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        end = time.time()
        secs = end - self.start
        msecs = secs * 1000  # millisecs
        if self.verbose:
            print '(%s) elapsed time: %f ms' % (self.key,msecs)
        if self.key:
            if self.key not in self.__class__.measures:
                self.__class__.measures[self.key]=faststat.Stats()
            if msecs>=0:
                self.__class__.measures[self.key].add(msecs)
            

    @classmethod
    def printStats(cls):
        print "\n -------------------------"
        print "  Timing stats:" 
        pprint.pprint(cls.getStats())
        print "\n -------------------------"


    @classmethod
    def getStats(cls):
        stats={}
        for m in cls.measures:
            stats[m]={'avg':cls.measures[m].mean, 'calls':cls.measures[m].n, 'min':cls.measures[m].min, 'max':cls.measures[m].max}
        return stats

