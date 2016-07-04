
from odpw.new.db import DBClient
from odpw.new.services.aggregates import aggregate

from utils.timer import Timer

if __name__ == '__main__':
    db= DBClient(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    snapshot=1625
    portalid='opendata_hu'

    aggregate(db, snapshot)


    Timer.printStats()
