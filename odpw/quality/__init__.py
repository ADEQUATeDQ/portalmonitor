from abc import abstractmethod
from collections import defaultdict

import structlog
log = structlog.get_logger()
from pybloom import ScalableBloomFilter





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

class DistinctElementCount(Analyser):
    def __init__(self, withDistinct=None):
        super(DistinctElementCount, self).__init__()
        self.count=0
        self.bloom=None
        self.set=None
        if withDistinct:
            self.bloom=ScalableBloomFilter(error_rate=0.00001)
            self.distinct=0
            self.set=set([])



    def getResult(self):
        res= {'count':self.count}
        if self.bloom is not None:
            res['distinct']=self.distinct
        return res


class ElementCountAnalyser(Analyser):
    """
    Provides a count per distinct element
    """
    def __init__(self, funct=None):
        self.dist=defaultdict(int)
        self.funct=funct

    def analyse_generic(self, element):
        if self.funct is not None:
            self.add(self.funct(element))
        else:
            self.add(element)

    def add(self, value, count=1):
        self.dist[value] += count

    def getDist(self):
        return dict(self.dist)

    def getResult(self):
        return self.getDist()

# class CKANDMD(Analyser):
#     def __init__(self):
#         super(CKANDMD, self).__init__()
#         self.analysers = ckan_analyser()
#
#     def analyse_Dataset(self,dataset):
#         if hasattr(dataset,'dmd'):
#             dataset.dmd['ckan'] = {}
#         else:
#             dataset.dmd={'ckan': {}}
#         for id, a in self.analysers:
#             try:
#                 res = a.analyse(dataset)
#                 if res:
#                     dataset.dmd['ckan'][id] = res
#             except Exception as e:
#                 ErrorHandler.handleError(log, "CkanAnalyserException", analyser=id, exception=e, exc_info=True)



