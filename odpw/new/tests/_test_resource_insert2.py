from multiprocessing import Process, cpu_count, Manager
from os import sys
import time
import logging
from Queue import Empty

from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import asc

from new.model import ResourceInfo
from utils.timer import Timer

logger = logging.getLogger('chat_message_parser')
logger.setLevel(logging.DEBUG)
logging.basicConfig()
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
logger.addHandler(stream_handler)
from odpw.db.dbm import PostgressDBM

from odpw.new.db import DBClient,DBManager
import structlog


log =structlog.get_logger()


def producer_queue(queue, snapshot):
    from odpw.db.models import Resource
    dbm=PostgressDBM(user='opwu', password='0pwu', host='portalwatch.ai.wu.ac.at', port=5432, db='portalwatch')
    iter=Resource.iter(dbm.getResources(snapshot=snapshot))

    for R in iter:
        uri=R.url
        uri=uri.replace("http:// \thttp:","http:")
        uri=uri.replace("http:// http://","http://")

        r={
            'snapshot':R.snapshot
            ,'uri':uri
            ,'timestamp':R.timestamp
            ,'status':R.status
            ,'exc':R.exception
            ,'header':R.header
            ,'mime':R.mime
            ,'size':R.size
        }
        RI=ResourceInfo(**r)
        queue.put(RI)

    queue.put('STOP')


def consumer_queue(proc_id, queue):
    dbm= DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    #dbm= DBManager(user='opwu', password='0pwu', host='datamonitor-data.ai.wu.ac.at', port=5432, db='portalwatch')
    db= DBClient(dbm)

    batch = []
    while True:
        try:
            time.sleep(0.01)
            RI = queue.get(proc_id, 1)
            if RI == 'STOP':
                logger.info('STOP received')
                # put stop back in queue for other consumers
                queue.put('STOP')
                break

            if not db.resourceinfoExists(RI.uri, RI.snapshot):
                if db.metaresourceExists(RI.uri):
                    batch.append(RI)
                else:
                    log.warn("URI missing", uri=RI.uri)

            batch.append(RI)
            if queue.qsize() > 1000:
                for i in xrange(50):
                    RI = queue.get(proc_id, 1)
                    if not db.resourceinfoExists(RI.uri,RI.snapshot):
                        if db.metaresourceExists(RI.uri):
                            batch.append(RI)
                        else:
                            log.warn("URI missing", uri=RI.uri)

            if len(batch)==100:
                with Timer(key="Batch"+str(proc_id), verbose=True):
                    db.bulkadd(batch)
                    batch=[]
                    print queue.qsize()
        except Empty:
            pass

if __name__ == '__main__':
    #dbm= DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    #dbm= DBManager(user='opwu', password='0pwu', host='datamonitor-data.ai.wu.ac.at', port=5432, db='portalwatch')
    #db= DBClient(dbm)

    manager = Manager()
    queue = manager.Queue()
    NUMBER_OF_PROCESSES = cpu_count()

    #NUMBER_OF_PROCESSES=1
    #snapshot=1625
    snapshots=[1622]
    #snapshots=[1619]
    Ps=[]
    tasks=[]
    for sn in snapshots:
        tasks.append(sn)

    producer = Process(
            target=producer_queue,
            args=(queue, snapshots[0])
        )
    producer.start()

    consumers = [
            Process(target=consumer_queue, args=(i, queue,))
            for i in xrange(NUMBER_OF_PROCESSES)
        ]
    for consumer in consumers:
        consumer.start()


    producer.join()
    for consumer in consumers:
        consumer.join()

    #pool = Pool(1)
    #for x in pool.imap(migrate,tasks):
    #    sn=x
    #    log.info("RECEIVED RESULT", snapshot=sn)

        #df= aggregateByPortal3(db, x[0].id, x[1])
        #dfm=df.mean().round(decimals=2).copy()
        #data={k:float(str(v)) for k,v  in dict(dfm).items()}
        #data['datasets']=df.shape[0]
        #PSQ= PortalSnapshotQuality(portalid=x[0].id, snapshot=x[1], **data)
        #db.add(PSQ)

    #results = pool.map_async( migrate, tasks, getResult )
    #pool.close()
    #pool.join()
