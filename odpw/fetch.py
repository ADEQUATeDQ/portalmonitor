__author__ = 'jumbrich'

from db.POSTGRESManager import PostGRESManager
from db.models import Portal

import logging
logger = logging.getLogger(__name__)

def name():
    return 'Fetch'
def setupCLI(pa):
    gilter = pa.add_argument_group('filters', 'filter option')
    gilter.add_argument('-d','--datasets',type=int, dest='ds', help='filter portals with more than specified datasets')
    gilter.add_argument('-r','--resources',type=int, dest='res')
    gilter.add_argument('-s','--software',choices=['CKAN'], dest='software')
    pa.add_argument("--force", action='store_true', help='force a full fetch, otherwise use update')
    pa.add_argument("-sn","--snapshot", required=True, help='what snapshot is it')

def cli(args):
    dbm = PostGRESManager(host=args.dbhost)

    for portal in dbm.getPortals(maxDS=args.ds, maxRes=args.res, software=args.software):
        print portal