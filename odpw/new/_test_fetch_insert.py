import datetime
import json

import multiprocessing
import rdflib
import urlnorm

from odpw.new.services.fetch_insert import fetchMigrate
from odpw.db.dbm import PostgressDBM
from odpw.analysers.quality.analysers import DCATDMD

from odpw.new.db import DBClient
from odpw.new.model import Portal
from odpw.new.portal_fetch_processors import CKAN

from odpw.utils.timer import Timer

import structlog


log =structlog.get_logger()

db= DBClient(user='opwu', password='0pwu', host='datamonitor-data.ai.wu.ac.at', port=5432, db='portalwatch')
dbm=PostgressDBM(user='opwu', password='0pwu', host='portalwatch.ai.wu.ac.at', port=5432, db='portalwatch')

def migrate(P,snapshot):
    #dbm.engine.dispose()
    #db.engine.dispose()


    #P=db.Session.query(Portal).filter(Portal.id==Pid).first()
    fetchMigrate(P,snapshot, db, dbm)

if __name__ == '__main__':
    #db= DBClient(user='opwu', password='0pwu', host='portalwatch.ai.wu.ac.at', port=5432, db='portalwatch')

    #db.init(Base)
    ckan= CKAN()

    snapshot=1625
    Ps=[]
    tasks=[]
    for P in db.Session.query(Portal):
        if P.id!='data_gv_at':
            Ps.append(P)
        if P.id!='data_gv_at':
            tasks.append((P, snapshot))

    #P = db.session.query(Portal).filter(Portal.id==P.id).first()



    for P in Ps:
        fetchMigrate(P,snapshot, db, dbm)
        Timer.printStats()

    #print tasks
    #pool = multiprocessing.Pool(2)
    #results = [pool.apply_async( migrate, t ) for t in tasks]
    #for result in results:
    #    result.get()
