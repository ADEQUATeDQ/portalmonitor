from faststat.faststat import _sigfigs

__author__ = 'jumbrich'


import time
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
        print '>>>','--*'*10,'EXCEPTIONS','*--'*10
        s=[" Timing stats:","\n"]
        for m,st in Timer.measures.items():
            p = st.percentiles
            if st.n < len(p):
                quartiles = "(n too small)"
            else:
                quartiles = (_sigfigs(p.get(0.25, -1)),
                    _sigfigs(p.get(0.5, -1)), _sigfigs(p.get(0.75, -1)))
            d = ["  ["+m+'] -', str(st.mean), 'avg ms for',m,str(st.n),'calls)'
                    ,"\n       (min:",str(st.min),"-",str(st.max),":max, quantils:",quartiles,")\n"]
            s=s+d
        s=s+['-'*50]
        print " ".join(s)
        print '<<<','--*'*10,'EXCEPTIONS','*--'*10

    @classmethod
    def getStats(cls):

        stats={}
        for m,st in cls.measures.items():
            p = st.percentiles
            stats[m]={
                'avg':st.mean
                , 'calls':st.n
                , 'min':st.min
                , 'max':st.max
                , 'q25':None, 'q5':None, 'q75':None
            }
            if st.n < len(p):
                quartiles = "(n too small)"
            else:
                stats[m]['q25']=_sigfigs(p.get(0.25, -1))
                stats[m]['q5']=_sigfigs(p.get(0.5, -1))
                stats[m]['q75']=_sigfigs(p.get(0.75, -1))
        return stats


def timer(delta):
    hours, rem = divmod(delta, 3600)
    minutes, seconds = divmod(rem, 60)
    return ("{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))
