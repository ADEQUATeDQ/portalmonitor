from odpw.core.api import DBClient
from odpw.core.model import Portal
from odpw.resources.portals_to_json import _calc_id

import structlog
log = structlog.get_logger()

def help():
    return "Add a new portal to the DB"
def name():
    return 'AddPortal'

def setupCLI(pa):
    pa.add_argument(
        '-u', '--uri',
        help="Portal uri", required=True,
        action='store', dest='uri'
    )
    pa.add_argument(
        '-a', '--api',
        help="Portal api uri", required=True,
        action='store', dest='api'
    )
    pa.add_argument(
        '-s', '--spftware',
        help="Portal software", required=True,
        action='store', dest='software',choices=['CKAN', 'OpenDataSoft', 'Socrata']
    )
    pa.add_argument(
        '-i', '--iso',
        help="Portal iso2 code ", required=True,
        action='store', dest='iso'
    )
    pass

def cli(args,dbm):

    api=DBClient(dbm)
    P = Portal(
                id=_calc_id(args.uri)
                ,uri=args.uri
                ,apiuri = args.api
                ,software = args.software
                ,iso = args.iso
                ,active= True
                )
    api.add(P)
    log.info("Added portal", uri=args.uri, api=args.api, software=args.software, iso=args.iso)