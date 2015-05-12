__author__ = 'jumbrich'


import sys
from db.models import Portal
from db.POSTGRESManager import PostGRESManager

import logging
logger = logging.getLogger(__name__)

def name():
    return 'Init'
def setupCLI(pa):
    pa.add_argument('-p','--portals',type=file, dest='plist')
    pa.add_argument('-s','--software',choices=['CKAN'], dest='software')
    pa.add_argument('-db','--db',  action='store_true', dest='dbinit')

def cli(args):

    dbm = PostGRESManager(host=args.dbhost)
    if args.dbinit:
        while True:
            choice = raw_input("WARNING: Do you really want to init the DB? (This destroys all data): (Y/N)").lower()
            if choice == 'y':
                logger.info("Reseting DB now")
                dbm.initTables()
                break
            elif choice == 'n':
                logger.info("Thought so :)")
                break
            else:
                sys.stdout.write("Please respond with 'y' or 'n' \n")

    if args.plist:
        for l in args.plist:
            if len(l.split(","))==2 and len(l.split(",")[1].strip())>0:
                p = Portal.newInstance(url=l.split(",")[0].strip(), apiurl=l.split(",")[1].strip())

                dbm.insertPortal(p)
