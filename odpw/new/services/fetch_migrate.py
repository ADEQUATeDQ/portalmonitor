import datetime
from multiprocessing import Pool

import structlog
log =structlog.get_logger()



from odpw.db.dbm import PostgressDBM

from odpw.new.services.aggregates import aggregatePortalQuality

from odpw.new.utils.helper_functions import readDBConfFromFile, md5
from odpw.new.utils.error_handling import ErrorHandler
from odpw.new.services.fetch_insert import insertDatasets


from odpw.new.core.model import Dataset, PortalSnapshot, MetaResource, Portal
from odpw.new.core.db import DBClient, DBManager



def fetchMigrate(obj):
    try:

        P, dbConf, snapshot = obj[0],obj[1],obj[2]

        dbm = DBManager(**dbConf)
        db= DBClient(dbm)


        dbm1=PostgressDBM(user='opwu', password='0pwu', host='portalwatch.ai.wu.ac.at', port=5432, db='portalwatch')
        PMD= dbm1.getPortalMetaData(portalID=P.id, snapshot=snapshot)
        if PMD is None:
            log.info("Skipping ",portalid=P.id, snapshot=snapshot)
            return (P, snapshot)
        PS= PortalSnapshot(portalid=P.id, snapshot=snapshot)

        if PMD.fetch_stats is not None and 'fetch_start' in PMD.fetch_stats:

            start= datetime.datetime.strptime(PMD.fetch_stats['fetch_start'], "%Y-%m-%dT%H:%M:%S.%f")
            end= datetime.datetime.strptime(PMD.fetch_stats['fetch_end'], "%Y-%m-%dT%H:%M:%S.%f") if 'fetch_end' in PMD.fetch_stats and PMD.fetch_stats['fetch_end'] is not None else None
            PS.start=start
            PS.end=end
            PS.exc=PMD.fetch_stats['exception']
            PS.status=PMD.fetch_stats['status']
        else:
            PS.start=None
            PS.end=None
        db.add(PS)

        from odpw.db.models import Dataset as DDataset

        iter=DDataset.iter(dbm1.getDatasetsAsStream(portalID=P.id, snapshot=snapshot))
        insertDatasets(P,db, iter,snapshot)
        try:
            s=db.Session
            PS= s.query(PortalSnapshot).filter(PortalSnapshot.portalid==P.id, PortalSnapshot.snapshot==snapshot).first()
            PS.datasetCount= s.query(Dataset).filter(Dataset.snapshot==snapshot).filter(Dataset.portalid==P.id).count()
            PS.resourceCount=s.query(Dataset).filter(Dataset.snapshot==snapshot).filter(Dataset.portalid==P.id).join(MetaResource,MetaResource.md5==Dataset.md5).count()
            s.commit()
            s.remove()
        except Exception as exc:
            ErrorHandler.handleError(log, "UpdatePortalSnapshot", exception=exc, pid=P.id, snapshot=snapshot, exc_info=True)
        try:
            aggregatePortalQuality(db,P.id, snapshot)
        except Exception as exc:
            ErrorHandler.handleError(log, "PortalFetchAggregate", exception=exc, pid=P.id, snapshot=snapshot, exc_info=True)

        print P, snapshot
        return (P, snapshot)
    except Exception as exc:
        ErrorHandler.handleError(log, "NoIdeaWhat happend", exception=exc, pid=P.id, snapshot=snapshot, exc_info=True)
        return (P, snapshot)

#--*--*--*--*
def help():
    return "perform head lookups"
def name():
    return 'FetchM'

def setupCLI(pa):
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=4)
    pa.add_argument('--pid', dest='portalid' , help="Specific portal id ")
    pa.add_argument('--sn', dest='snapshot' , help="Snapshot")

def cli(args,dbm):
    sn=args.snapshot

    dbConf= readDBConfFromFile(args.config)
    db= DBClient(dbm)

    tasks=[]
    if args.portalid is not None:
        P = db.Session.query(Portal).filter(Portal.id==args.portalid).one()
        if P is None:
            log.warn("PORTAL NOT IN DB", portalid=args.portalid)
            return
        else:
            log.info("ADDING", portalid=P.id, snapshot=sn)
            tasks.append((P, dbConf, sn))
    else:
        for P in db.Session.query(Portal):
            log.info("ADDING", portalid=P.id, snapshot=sn)
            tasks.append((P, dbConf, sn))

    log.info("START FETCH", processors=args.processors, dbConf=dbConf, portals=len(tasks))

    pool = Pool(args.processors)
    results = pool.map(fetchMigrate,tasks)
    pool.close()
    pool.join()
    print(results)
