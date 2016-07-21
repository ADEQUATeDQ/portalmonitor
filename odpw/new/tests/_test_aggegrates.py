
from new.core.db import DBClient, DBManager
from odpw.new.services.aggregates import aggregate, aggregatePortalQuality
from utils.timer import Timer

if __name__ == '__main__':
    dbm=DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    #dbm= DBManager(user='opwu', password='0pwu', host='datamonitor-data.ai.wu.ac.at', port=5432, db='portalwatch')
    db= DBClient(dbm)
    snapshot=1628
    #aggregate(db,snapshot)

    portalid='exploredata_gov_ro'
    aggregatePortalQuality(db,portalid,snapshot)


    Timer.printStats()
