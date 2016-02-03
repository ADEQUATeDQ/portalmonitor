'''
Created on Feb 3, 2016

@author: jumbrich
'''
import os
from os.path import join
from os import walk

class Page(object):
    
    def __init__(self, filename):
        self._title= os.path.basename(filename)
        self._rev_hist=self.parse(filename)
        
    def parse(self, filename):
        import json
        from pprint import pprint

        revs=[]
        with open(filename) as data_file:    
            data = json.load(data_file)

            #pprint(data)
            
            
            pages=data['query']['pages']
            for pID, val in pages.items():
                for rev in val['revisions']:
                    if 'minor' not in rev:
                        revs.append(rev['timestamp'])
                
        return sorted(revs)
        

if __name__ == '__main__':
    
    
    revs='revs'
    for (dirpath, dirnames, filenames) in walk(revs):
        for cat in dirnames:
            print cat
            for (dirpath, dirnames, filenames) in walk(join(dirpath,cat)):
                for fname in filenames:
                    
                    p = Page(join(dirpath,fname))
                    print p._rev_hist
                    break
            