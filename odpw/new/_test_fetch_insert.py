from multiprocessing import Pool

from odpw.new.services.fetch_insert import fetchMigrate
from odpw.db.dbm import PostgressDBM

from odpw.new.db import DBClient,DBManager
from odpw.new.model import Portal, PortalSnapshotQuality

from odpw.utils.timer import Timer

import structlog


log =structlog.get_logger()
import time
#dbm= DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
def migrate(obj):

    #dbm= DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    dbm= DBManager(user='opwu', password='0pwu', host='datamonitor-data.ai.wu.ac.at', port=5432, db='portalwatch')
    db= DBClient(dbm)
    time.sleep(1)
    P= obj[0]
    snapshot=obj[1]
    try:
        with Timer(key="TIMER:Migrate "+P.id, verbose=True):
            dbm1=PostgressDBM(user='opwu', password='0pwu', host='portalwatch.ai.wu.ac.at', port=5432, db='portalwatch')

            #P=db.Session.query(Portal).filter(Portal.id==Pid).first()
            #print "Fetching",P.id, "snapshot",snapshot
            fetchMigrate(P, snapshot, db, dbm1)
    except Exception as e:
        print "ERROR",e
        import traceback
        traceback.print_exc()
    db.remove()
    return (P, snapshot)

if __name__ == '__main__':
    #dbm= DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    dbm= DBManager(user='opwu', password='0pwu', host='datamonitor-data.ai.wu.ac.at', port=5432, db='portalwatch')
    db= DBClient(dbm)

    #snapshot=1625
    snapshots=[1627,1626,1625,1624,1623,1622,1621,1622]
    #snapshots=[1619]
    Ps=[]
    tasks=[]
    for P in db.Session.query(Portal):
        #if P.id=='data_gv_at':
            #Ps.append(P)
            #if P.id!='data_gv_at':
        for sn in snapshots:
            tasks.append((P, sn))

    pool = Pool(4)
    for x in pool.imap(migrate,tasks):
        pid=x[0].id
        sn=x[1]
        log.info("RECEIVED RESULT", portalid=pid, snapshot=sn)

        #df= aggregateByPortal3(db, x[0].id, x[1])
        #dfm=df.mean().round(decimals=2).copy()
        #data={k:float(str(v)) for k,v  in dict(dfm).items()}
        #data['datasets']=df.shape[0]
        #PSQ= PortalSnapshotQuality(portalid=x[0].id, snapshot=x[1], **data)
        #db.add(PSQ)

    #results = pool.map_async( migrate, tasks, getResult )
    #pool.close()
    #pool.join()

