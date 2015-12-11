from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.core import DCATConverter
from odpw.analysers.quality.new.dcat_accuracy import AccuracyDCATAnalyser
from odpw.db.models import Portal, Dataset, Resource
from odpw.utils.util import getSnapshot, progressIterator, ErrorHandler

import structlog
log =structlog.get_logger()

__author__ = 'sebastian'

def help():
    return "Compute accuracy metrics after head lookups"
def name():
    return 'Accuracy'
def setupCLI(pa):

    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument('-u','--url',type=str, dest='url', help="the API url")


def accuracy_calc(dbm, sn, portal):
    try:

        log.info("START Accuracy calculation", pid=portal.id, snapshot=sn, software=portal.software)
        print portal.id, sn

        # load all resources from db
        resources = {r.url: r for r in Resource.iter(dbm.getResources(portalID=portal.id, snapshot=sn))}
        print portal.id, 'resources loaded:', str(len(resources))

        total = dbm.countDatasets(snapshot=sn, portalID=portal.id)

        log.info("Computing accuracy metrics", sn=sn, count=total)
        steps= total/10
        if steps==0:
            steps = 1

        iter = progressIterator(Dataset.iter(dbm.getDatasets(snapshot=sn, portalID=portal.id)), total, steps)
        aset = AnalyserSet()
        aset.add(DCATConverter(portal))
        rsc = aset.add(AccuracyDCATAnalyser(resources))

        process_all(aset, iter)

        pmd = dbm.getPortalMetaData(snapshot=sn, portalID=portal.id)
        aset.update(pmd)
        dbm.updatePortalMetaData(pmd)

    except Exception as e:
        ErrorHandler.handleError(log, "AccuracyCalculationException", exception=e, portal=portal.id, snapshot=sn)


def cli(args,dbm):
    sn = getSnapshot(args)

    ps=[]
    if args.url:
        p = dbm.getPortal(apiurl=args.url)
        if p:
            ps.append(p.id)
    else:
        for p in Portal.iter(dbm.getPortals()):
            ps.append(p)
    for portal in ps:
        snapshots=set([])
        if not sn:
            for s in dbm.getSnapshots(portalID=portal.id):
                snapshots.add(s['snapshot'])
        else:
            snapshots.add(sn)

        for sn in sorted(snapshots):
            accuracy_calc(dbm, sn, portal)
