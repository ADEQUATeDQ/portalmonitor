import structlog

from core.api import DBClient
from core.db import DBManager
from odpw.core.portal_fetch_processors import CKAN
from odpw.core.model import Portal


log =structlog.get_logger()

if __name__ == '__main__':
    dbm= DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    #dbm= DBManager(user='opwu', password='0pwu', host='datamonitor-data.ai.wu.ac.at', port=5432, db='portalwatch')
    db= DBClient(dbm)


    portalid='www_europeandataportal_eu'
    snapshot=1629

    P=db.Session.query(Portal).filter(Portal.id==portalid).first()

    c=CKAN()
    iter = c.generateFetchDatasetIter(P,1111)
    for d in iter:
        print d
