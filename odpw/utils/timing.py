import sys
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
        print '>>>','--*'*10,'TIMER','*--'*10
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
        print '<<<','--*'*10,'TIMER','*--'*10

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


def progressIterator(iterable, total, steps, label=None):
    c=0
    start= time.time()
    start_interim=start
    for element in iterable:
        c+=1
        if c%steps ==0:
            elapsed = (time.time() - start)
            interim=(time.time()-start_interim)
            progressIndicator(c, total, elapsed=elapsed, interim=interim, label=label)
            start_interim=time.time()
            #Timer.printStats()
        yield element

def progressIndicator(processed, total, bar_width=20, elapsed=None, interim=None, label=None):

    if total!=0:
        percent = float(processed) / total
    else:
        percent =1.0
    hashes = '#' * int(round(percent * bar_width))
    spaces = ' ' * (bar_width - len(hashes))

    el_str=""
    if elapsed:
        el_str= "runtime: "+timer(elapsed)
        #str(timedelta(seconds=elapsed))
    it_str=""
    if interim:
        it_str="interim: "+timer(interim)
        #str(timedelta(seconds=interim))

    l= label if label else 'Progress'
    sys.stdout.write("\r{6}: {1}% [{0}] ({2}/{3}) {4} {5}".format(hashes + spaces, int(round(percent * 100)), processed, total, el_str,it_str, l))
    sys.stdout.flush()