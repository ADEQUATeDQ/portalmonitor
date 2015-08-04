__author__ = 'jumbrich'

import pandas
from odpw.utils.timer import Timer

import types
from abc import abstractmethod
from collections import OrderedDict

from _collections import defaultdict
import time
import numpy as np
import structlog
log = structlog.get_logger()


_analyserRegistrar={}
class AnalyserFactory(object):
    
    def __new__(cls, class_name, parents, attributes):
        print "Creating class", class_name
        # Here we could add some helper methods or attributes to c
        c = type(class_name, parents, attributes)
        print class_name
        if class_name not in _analyserRegistrar:
            _analyserRegistrar[class_name] = c
        return c

    @staticmethod
    def get_class_from_frame_identifier(class_name):
        return _analyserRegistrar.get(class_name)
    
    @staticmethod
    def createAnalyser(class_name, **kwargs):
        return _analyserRegistrar.get(class_name)(**kwargs)


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

    @abstractmethod
    def done(self): pass



class AnalyserSet(Analyser):
    
    def __init__(self, analysers=None, timing=False):
        self.analysers = OrderedDict()
        for a in analysers or []:
            if isinstance(a, Analyser):
                self.analysers[a.name()] = a
                
    def add(self, analyser):
        if isinstance(analyser, Analyser) and analyser.name() not in self.analysers: 
            self.analysers[analyser.name()] = analyser

    def analyse(self, element):
        for c in self.analysers.itervalues():
            c.analyse(element)
        
    def update(self, element):
        for c in self.analysers.itervalues():
            c.update(element)
    
    def done(self):
        for c in self.analysers.itervalues():
            c.done()

    def getAnalyser(self, analyser):
        if isinstance(analyser, (types.TypeType, types.ClassType)) and  issubclass(analyser, Analyser):
            return self.analysers[analyser.name()]
        elif isinstance(analyser, Analyser):
            return self.analysers[analyser.name()]
        
    def getAnalysers(self):
        return self.analysers.values()

class AnalyseEngine(Analyser):
    
    def __init__(self, convert=None):
        self.analysers = OrderedDict()
        self.convert = convert
        self.count=0
        
    def add(self, analyser):
        self.analysers[analyser.name()] = analyser

    def analyse(self, element):
        self.count+=1
        with Timer(key="analyse") as t:
            for c in self.analysers.itervalues():
                c.analyse(element)
        
    
    def update(self, element):
        with Timer(key="update") as t:
            for c in self.analysers.itervalues():
                c.update(element)
    
    
    def done(self):
        for c in self.analysers.itervalues():
            c.done()
        log.info("DONE Analysis", count=self.count)

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


def process_all( analyser, iterable):
    for e in iterable:
        analyser.analyse(e)
    analyser.done()
    
    

