'''
Created on Feb 3, 2016

@author: jumbrich
'''
import datetime
import os
from os.path import join
from os import walk
import json
from freshness.estimators import *
import json_compare
from dateutil.parser import parse

TS_QUERY = ['query', 'pages', '*', 'revisions', '*']
TITLE_QUERY = ['query', 'pages', '*', 'title']

class Page(object):
    
    def __init__(self, filename):
        self.filename = os.path.basename(filename)
        self.rev_hist = self.parse(filename)
        
    def parse(self, filename):
        """
        :return: the revision history of the file (without minor changes)
        """
        revs = []
        with open(filename) as data_file:    
            data = json.load(data_file)

            self.name = json_compare.select(TITLE_QUERY, data)[0]
            if json_compare.exists(TS_QUERY, data):
                revs = json_compare.select(TS_QUERY, data)[0]

        res = []
        for r in revs:
            try:
                if 'minor' not in r and 'timestamp' in r:
                    d = parse(r['timestamp'])
                    res.append(d)
            except Exception as e:
                print 'Parser Exception:', r, e

        return sorted(res)

    def iterExact(self):
        for t in self.rev_hist:
            yield t

    def iterAgeSampling(self, interval):
        i = self.rev_hist[0]
        end = self.rev_hist[-1]

        prev_t = i
        while i < end:
            for t in self.rev_hist:
                if t > i:
                    # return last modification date in this interval
                    yield prev_t
                    break
                prev_t = t

            # go to next interval
            i += interval
        yield prev_t

    def iterContentSampling(self, interval):
        prev_t = -1
        for t in self.iterAgeSampling(interval):
            if prev_t == t:
                yield 0
            else:
                yield 1
            prev_t = t

    def startTime(self):
        return self.rev_hist[0]

    def getDeltas(self):
        prev_r = None
        deltas = []
        for r in self.rev_hist:
            if prev_r:
                deltas.append((r - prev_r).total_seconds())
            prev_r = r
        return deltas


def content_sampling(page, interval, estimators):
    for Xi in page.iterContentSampling(interval):
        for e in estimators:
            e.update(Xi)

    I = interval.total_seconds()
    for e in estimators:
        e.setInterval(I)


def age_sampling(page, interval, estimators):
    I = interval.total_seconds()
    # the access time
    ACC = page.startTime()
    for t in page.iterAgeSampling(interval):
        # Ti is the time to the previous change in the ith access
        Ti = (ACC - t).total_seconds()
        for e in estimators:
            e.update(Ti, I)
        # set access time to next interval
        ACC += interval


if __name__ == '__main__':
    # Deltas
    # Avg change time

    revs='revs'
    for (dirpath, dirnames, filenames) in walk(revs):
        #for cat in dirnames:
        #    print cat
        for fname in filenames:

            p = Page(join(dirpath,fname))
            i = datetime.timedelta(days=10)

            c1 = IntuitiveFrequency()
            c2 = ImprovedFrequency()
            content_sampling(p, i, [c1, c2])
            e1 = c1.estimate()
            e2 = c2.estimate()
            print 'content based:', datetime.timedelta(seconds=1/e1), datetime.timedelta(seconds=1/e2)

            a1 = NaiveLastModified()
            a2 = ImprovedLastModified()
            age_sampling(p, i, [a1, a2])
            e1 = a1.estimate()
            e2 = a2.estimate()

            print 'age based:', datetime.timedelta(seconds=1/e1), datetime.timedelta(seconds=1/e2)
            print

            emp = EmpiricalDistribution(p.getDeltas())
            emp.plotDistribution()

            print

