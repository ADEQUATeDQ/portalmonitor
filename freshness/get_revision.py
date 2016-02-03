'''
Created on Feb 3, 2016

@author: jumbrich
'''
from os import listdir
from os.path import isfile, join
from os import walk
import os
import requests
import json

def getURL(title):
    return 'https://en.wikipedia.org/w/api.php?action=query&prop=revisions&titles='+title+'&rvprop=timestamp%7Cflags&rvlimit=max&format=json'
 
def fetchAndStoreRevision(title, dir):
    url = getURL(title)
    print 'GET', title
    r = requests.get(url)
    json_data= r.json()
    with open(join(dir,title), 'w') as f:
        json.dump(json_data, f)    
    
if __name__ == '__main__':
    
    folder='lists'
    revs='revs'
    for (dirpath, dirnames, filenames) in walk(folder):
        for file in filenames:
            d = join(revs,file.replace(".txt",""))
            if not os.path.exists(d):
                os.makedirs(d)
            
            with open(join(dirpath,file)) as f:
                c=0
                for title in f.readlines():
                    fname = join(d,title)
                    if c>0 and not os.path.isfile(fname):
                        fetchAndStoreRevision(title.rstrip(), d)                        
                    c+=1
 