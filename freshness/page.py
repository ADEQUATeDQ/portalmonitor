'''
Created on Feb 3, 2016

@author: jumbrich
'''
import datetime
import os
from os.path import join
from os import walk
import json
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


if __name__ == '__main__':
    # Deltas
    # Avg change time
    # No Minors

    
    revs='revs'
    for (dirpath, dirnames, filenames) in walk(revs):
        #for cat in dirnames:
        #    print cat
        for fname in filenames:

            p = Page(join(dirpath,fname))

            for t in p.iterExact():
                print t

            interval = datetime.timedelta(days=10)
            for a, b in zip(p.iterAgeSampling(interval), p.iterContentSampling(interval)):
                print a, b

            break
            