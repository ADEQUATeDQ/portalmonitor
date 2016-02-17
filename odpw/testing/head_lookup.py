'''
Created on Aug 13, 2015

@author: jumbrich
'''
from odpw.utils.timer import Timer

if __name__ == '__main__':
    import requests
    
    url='http://data.wu.ac.at/dataset/6fcfbf16-b4d0-4fe5-9bc3-706163d88819/resource/7ab87d96-d980-4fb4-be39-cc73c2ec6864/download/allcoursesandevents14w.csv'
    
    with Timer(verbose=True) as t:
        h= requests.get(url=url)
        print h.headers
    
    
    with Timer(verbose=True) as t:
        h= requests.head(url=url,headers={'Accept-Encoding': 'identity'})
        print h.headers
        
    import urllib, os
    with Timer(verbose=True) as t:
        site = urllib.urlopen(url)
        meta = site.info()
        print meta