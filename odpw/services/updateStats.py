

from odpw.core.api import DBClient
from odpw.services.aggregates import aggregateFormatDist
from odpw.utils.helper_functions import readDBConfFromFile
from odpw.utils.utils_snapshot import getCurrentSnapshot

import structlog
log =structlog.get_logger()


#--*--*--*--*
def help():
    return "perform head lookups"
def name():
    return 'UpdateStats'

def setupCLI(pa):
    #pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=4)
    #pa.add_argument('--pid', dest='portalid' , help="Specific portal id ")
    pass

def cli(args,dbm):
    sn = getCurrentSnapshot()

    dbConf= readDBConfFromFile(args.config)
    db= DBClient(dbm)

    aggregateFormatDist(db, sn)

