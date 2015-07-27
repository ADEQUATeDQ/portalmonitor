from odpw.db.models import *

import pandas
from odpw.timer import Timer
from odpw.util import timer
import types
from abc import abstractmethod


__author__ = 'jumbrich'

from _collections import defaultdict
import time
import numpy as np


### converter functions

def toPortal(e):
    try:
        return Portal.fromResult(dict(e))
    except Exception as ex:
        return None




class Analyser(object):
    
    @classmethod
    def name(cls): return cls.__name__
    
    def analyse(self, node, *args, **kwargs):
        meth = None
        for cls in node.__class__.__mro__:
            meth_name = 'analyse_' + cls.__name__
            meth = getattr(self, meth_name, None)
            if meth:
                break

        if not meth:
            meth = self.analyse_generic
        return meth(node, *args, **kwargs)

    @abstractmethod
    def analyse_generic(self, element): pass
    
    def update(self, node, *args, **kwargs):
        meth = None
        for cls in node.__class__.__mro__:
            meth_name = 'update_' + cls.__name__
            meth = getattr(self, meth_name, None)
            if meth:
                break

        if not meth:
            meth = self.update_generic
        return meth(node, *args, **kwargs)

    @abstractmethod
    def update_generic(self, element): pass
    
    
    @abstractmethod
    def getResult(self): pass
    def getDataFrame(self): pass
    
    def done(self): pass




class AnalyseEngine(Analyser):
    
    def __init__(self, convert=None):
        self.analysers = {}
        self.convert = convert
        
    def add(self, analyser):
        print "ADDED",analyser
        self.analysers[analyser.name()] = analyser

    def analyse(self, element):
        with Timer(key="analyse") as t:
            for c in self.analysers.itervalues():
                c.analyse(element)
    
    def done(self):
        for c in self.analysers.itervalues():
            print "DONE", c
            c.done()
        #self.end=time.time()
        #print 'AnalyseEngine elapsed time: %s (%f ms)' % (timer(self.end-self.start),(self.end-self.start)*1000)
    
    def process_all(self, iterable):
        self.start= time.time()
        for e in iterable:
            c=e
            if self.convert:
                c=self.convert(e)
            if c:
                self.analyse(c)
        self.done()
            
    def getAnalyser(self, analyser):
        if isinstance(analyser, (types.TypeType, types.ClassType)) and  issubclass(analyser, Analyser):
            return self.analysers[analyser.name()]
        elif isinstance(analyser, Analyser):
            return self.analysers[analyser.name()]
        
    def getAnalysers(self):
        return self.analysers.values()
    
class QualityAnalyseEngine(AnalyseEngine):
    pass