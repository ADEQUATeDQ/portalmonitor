'''
Created on Dec 18, 2015

@author: jumbrich
'''
from flask.ext.cache import Cache
cache = Cache(config={'CACHE_TYPE': 'simple'})
