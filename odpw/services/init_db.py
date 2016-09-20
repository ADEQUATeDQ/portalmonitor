from odpw.core.model import Base
import structlog
log = structlog.get_logger()

def help():
    return "Initalise the DB"
def name():
    return 'InitDB'

def setupCLI(pa):
    pass

def cli(args,dbm):
    log.info('Initalising the DB')
    dbm.init(Base)
    log.info('DB Initalised')