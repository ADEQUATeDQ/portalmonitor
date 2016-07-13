from multiprocessing import Pool

from new.services.resourceinfo_insert import resourceMigrate
from odpw.new.services.fetch_insert import fetchMigrate
from odpw.db.dbm import PostgressDBM

from odpw.new.db import DBClient,DBManager
from odpw.new.model import Portal, PortalSnapshotQuality

from odpw.utils.timer import Timer

import structlog


log =structlog.get_logger()
import time
#dbm= DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
def migrate(snapshot):

    dbm= DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    #dbm= DBManager(user='opwu', password='0pwu', host='datamonitor-data.ai.wu.ac.at', port=5432, db='portalwatch')
    db= DBClient(dbm)
    time.sleep(1)
    try:
        with Timer(key="TIMER:Migrate "+str(snapshot), verbose=True):
            dbm1=PostgressDBM(user='opwu', password='0pwu', host='portalwatch.ai.wu.ac.at', port=5432, db='portalwatch')

            #P=db.Session.query(Portal).filter(Portal.id==Pid).first()
            #print "Fetching",P.id, "snapshot",snapshot
            resourceMigrate( snapshot, db, dbm1)
    except Exception as e:
        print "ERROR",e
        import traceback
        traceback.print_exc()
    db.remove()
    return snapshot

if __name__ == '__main__':
    dbm= DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    #dbm= DBManager(user='opwu', password='0pwu', host='datamonitor-data.ai.wu.ac.at', port=5432, db='portalwatch')
    db= DBClient(dbm)

    #snapshot=1625
    snapshots=[1622]
    #snapshots=[1619]
    Ps=[]
    tasks=[]
    for sn in snapshots:
        tasks.append(sn)

    pool = Pool(1)
    for x in pool.imap(migrate,tasks):
        sn=x
        log.info("RECEIVED RESULT", snapshot=sn)

        #df= aggregateByPortal3(db, x[0].id, x[1])
        #dfm=df.mean().round(decimals=2).copy()
        #data={k:float(str(v)) for k,v  in dict(dfm).items()}
        #data['datasets']=df.shape[0]
        #PSQ= PortalSnapshotQuality(portalid=x[0].id, snapshot=x[1], **data)
        #db.add(PSQ)

    #results = pool.map_async( migrate, tasks, getResult )
    #pool.close()
    #pool.join()

