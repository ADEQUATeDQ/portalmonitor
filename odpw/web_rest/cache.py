'''
Created on Dec 18, 2015

@author: jumbrich
'''
from flask_caching import Cache
cache = Cache(config={'CACHE_TYPE': 'simple'})
