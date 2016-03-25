import json
import pickle

import os
import rdflib

from odpw.analysers.core import DCATConverter
from odpw.utils.dataset_converter import graph_from_opendatasoft, fix_socrata_graph, CKANConverter

__author__ = 'jumbrich'


from odpw.db.models import Portal, Dataset

from odpw.analysers import  process_all, SAFEAnalyserSet, Analyser


import structlog
log =structlog.get_logger()

def name():
    return 'Meta'

def help():
    return "Extract metadata files"

def setupCLI(pa):
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument('-o','--out',type=str, dest='out' , help="the out directory for the list of urls (and downloads)")
    pa.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")
    pa.add_argument('--iso',type=str, dest='iso' , help="isoCode")
    pa.add_argument('-f','--filter',type=str, dest='filter' , help="Filter by format (csv)", default='csv')
    pa.add_argument('--store',  action='store_true', default=False, help="store the files in the out directory")

def cli(args, dbm):


    if args.out is None:
        raise IOError("--out is not set")
    if args.snapshot is None:
        raise IOError("--snapshot not set")

    if not os.path.exists(args.out):
        os.makedirs(args.out)
        print "'writing to",args.out

    if args.url:
        p = dbm.getPortal(url=args.url)
        if not p:
            raise IOError(args.url + ' not found in DB')
        portals = [p]
    elif args.iso:
        portals = [p for p in Portal.iter(dbm.getPortals(iso3=args.iso))]
    else:
        portals = [p for p in Portal.iter(dbm.getPortals())]


    for p in portals:
        try:
            extract(p, dbm, args.snapshot, args.out)
        except Exception as e:
            print 'error in portal:', p.url
            print e


def extract(portal, dbm, snapshot, out):
    log.info("Extracting DCAT mappings from ", portals=portal.id)

    ae = SAFEAnalyserSet()
    ae.add(DCATConverter(portal))
    dcatlist=ae.add(DCATStore(portal))
    metalist=ae.add(MetaStore(portal))

    iter = Dataset.iter(dbm.getDatasetsAsStream(portal.id, snapshot=snapshot))
    process_all( ae, iter)


    sndir=os.path.join(out, snapshot)
    if not os.path.exists(sndir):
        os.makedirs(sndir)

    pdir=os.path.join(sndir, portal.id)
    if not os.path.exists(pdir):
        os.makedirs(pdir)

    fname='dcat'
    with open(os.path.join(pdir, fname+ '.pkl'), 'wb') as f:
        pickle.dump(dcatlist.getResult(), f)
        print 'Writing dict to ',f
    with open(os.path.join(pdir, fname+'.json'), 'w') as f:
        json.dump(dcatlist.getResult(), f)
        print 'Writing dict to ',f


    fname='metadata'
    with open(os.path.join(pdir, fname+ '.pkl'), 'wb') as f:
        pickle.dump(metalist.getResult(), f)
        print 'Writing dict to ',f
    with open(os.path.join(pdir, fname+'.json'), 'w') as f:
        json.dump(metalist.getResult(), f)
        print 'Writing dict to ',f


class MetaStore(Analyser):
    def __init__(self):
        self.results=[]


    def analyse_Dataset(self, dataset):
        self.results.append(dataset.data)

    def getResult(self):
        return self.results

class DCATStore(Analyser):

    def __init__(self):
        self.results=[]


    def analyse_Dataset(self, dataset):

        if hasattr(dataset,'dcat'):
            self.results.append(dataset.dcat)

    def getResult(self):
        return self.results






