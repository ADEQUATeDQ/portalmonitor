import dateutil

import datetime
import random

from isoweek import Week

from estimators import AgeSamplingEmpiricalDistribution, ImprovedLastModified, NaiveLastModified, delta_to_days, \
    MarkovChain
from odpw.core.api import DBClient
from odpw.core.db import DBManager
from odpw.core.model import Portal, ResourceHistory

from multiprocessing import Pool

import structlog

from odpw.utils.error_handling import ErrorHandler, getExceptionCode, getExceptionString
from odpw.utils.helper_functions import readDBConfFromFile
from odpw.utils.utils_snapshot import getCurrentSnapshot

log = structlog.get_logger()

def help():
    return "Compute freshness scores for resources"

def name():
    return 'Freshness'

def setupCLI(pa):
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=4)
    pa.add_argument('--pid', dest='portalid', help="Specific portal id ")


def cli(args,dbm):
    sn = getCurrentSnapshot()

    dbConf= readDBConfFromFile(args.config)
    db= DBClient(dbm)

    tasks=[]
    if args.portalid:
        P =db.Session.query(Portal).filter(Portal.id==args.portalid).one()
        if P is None:
            log.warn("PORTAL NOT IN DB", portalid=args.portalid)
            return
        else:
            tasks.append((P, dbConf,sn))
    else:
        for P in db.Session.query(Portal):
            tasks.append((P, dbConf,sn))

    log.info("START FRESHNESS", processors=args.processors, dbConf=dbConf, portals=len(tasks))

    pool = Pool(args.processors)
    for x in pool.imap(change_history,tasks):
        pid,sn =x[0].id, x[1]
        log.info("RECEIVED RESULT", portalid=pid, snapshot=sn)


def change_history(obj):
    P, dbConf, snapshot = obj[0],obj[1],obj[2]
    log.info("ChangeHistory", portalid=P.id, snapshot=snapshot)

    dbm = DBManager(**dbConf)
    db = DBClient(dbm)

    try:
        for res in db.getMetaResource(snapshot, portalid=P.id):
            # metadata modification date
            meta_lm = res.modified
            if meta_lm:
                reshist = ResourceHistory(uri=res.uri, snapshot=snapshot, modified=meta_lm, source='metadata')
                db.add(reshist)
            resInfo = db.getResourceInfoByURI(uri=res.uri, snapshot=snapshot).one()
            header = resInfo.header
            header_lm = None
            if 'last-modified' in header:
                header_lm = header['last-modified'][0]
            elif 'Last-Modified' in header:
                header_lm = header['Last-Modified'][0]
            try:
                header_lm = dateutil.parser.parse(header_lm)
            except Exception as e:
                header_lm = None
            if header_lm:
                reshist = ResourceHistory(uri=res.uri, snapshot=snapshot, modified=header_lm, source='header')
                db.add(reshist)

            # TODO comparison based: ETag

            # compute freshness score
            # TODO select source for change information
            changes = list(db.getResourcesHistory(uri=res.uri, source='header'))
            scores = freshness_score(changes, snapshot)
            print scores


        status = 200
        exc = None
    except Exception as exc:
        ErrorHandler.handleError(log, "FreshnessException", exception=exc, pid=P.id, snapshot=snapshot, exc_info=True)
        status = getExceptionCode(exc)
        exc = getExceptionString(exc)

    return (P, snapshot)



def freshness_score(changes, obj_sn, MIN_HIST=10):
    if len(changes) > MIN_HIST:
        a1 = NaiveLastModified()
        a2 = ImprovedLastModified()
        emp = AgeSamplingEmpiricalDistribution()
        estimators = [a1, a2, emp]
        mar1 = MarkovChain(history=1)
        mar2 = MarkovChain(history=2)
        mark_est = [mar1, mar2]
        for e in mark_est:
            e.setInterval(7)

        start_sn = changes[0].snapshot
        end_sn = changes[-1].snapshot

        y = int('20' + str(start_sn)[:2])
        w = int(str(start_sn)[2:])
        ACC = Week(y, w).thursday()
        prev_sn = None
        prev_t = 0

        for h in changes:
            sn = h.snapshot
            t = h.modified.date()
            if prev_sn and not (str(sn).startswith('16') and str(prev_sn).startswith('15')):
                interval = datetime.timedelta(days=(sn - prev_sn) * 7)
            else:
                interval = datetime.timedelta(days=7)

            # comparison based
            change = 1 if t != prev_t else 0
            # update new values
            for e in mark_est:
                e.update(change)

            I = delta_to_days(interval)

            # Ti is the time to the previous change in the ith access
            Ti = delta_to_days(ACC - t)

            for e in estimators:
                e.update(Ti, I, t)
            prev_sn = sn
            prev_t = t
            # set access time to next interval
            ACC += interval


        y = int('20' + str(obj_sn)[:2])
        w = int(str(obj_sn)[2:])

        # delta between thursday of objective snapshot and last observed modification
        current = delta_to_days(Week(y, w).thursday() - t)
        # compute the number of weeks to the objective snapshot, however at least 1 required
        delta = max(1, int(round(current / 7.)))

        return {'a_cho_naive': 1 - a1.cdf_poisson(current), 'a_cho_impr': 1 - a2.cdf_poisson(current),
                'a_emp_dist': 1 - emp.cdf(current),
                'mark_1': 1 - mar1.cumm_percent(delta), 'mark_2': 1- mar2.cumm_percent(delta),
                'snapshots': len(changes)}

            #hist_iter = db.getResourcesHistory(uri=uri, source='metadata')



def generate_testdata(db, P, snapshot, num_of_snapshot=10):
    for res in db.getMetaResource(snapshot, portalid=P.id):
        d = None
        for sn in range(snapshot-num_of_snapshot, snapshot):

            y = int('20' + str(sn)[:2])
            w = int(str(sn)[2:])
            new_d = Week(y, w).monday() - datetime.timedelta(days=random.randint(1, 5))
            if random.randint(1, 3) % 2 == 1 or not d:
                d = new_d

            reshist = ResourceHistory(uri=res.uri, snapshot=sn, modified=d, source='header')
            db.add(reshist)
