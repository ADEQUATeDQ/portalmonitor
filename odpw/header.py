__author__ = 'jumbrich'

import requests
from db.models import Resource

def headerLookup(resUrl=None, pid=None, snapshot=None, dbm=None, did=None):

    # check if we analysed this resource already

