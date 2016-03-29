import json
import pickle

import os
import rdflib

from odpw.analysers.core import DCATConverter
from odpw.utils.dataset_converter import graph_from_opendatasoft, fix_socrata_graph, CKANConverter
from odpw.utils.util import ErrorHandler

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

    if not os.path.exists(args.out):
        os.makedirs(args.out)
        print "'writing to",args.out


    if args.url:
        p = dbm.getPortal(url=args.url)
        if not p:
            raise IOError(args.url + ' not found in DB')
        portals = [p.id]

    snapshots=[]
    if args.snapshot:
        snapshots.append(args.snapshot)
    else:
        for sn in dbm.getSnapshotsFromPMD():#portalID='data_wu_ac_at'
            snapshots.append(sn[1])

    for sn in sorted(snapshots):
        sn_portals=[]
        for portalID in dbm.getPortalIDs(snapshot=sn):
            sn_portals.append(portalID)

        for portalID in sn_portals:
            pID = portalID[0]
            p=dbm.getPortal(portalID=pID)
            try:
                extract(p, dbm, str(sn), args.out)
            except Exception as e:
                ErrorHandler.handleError(log, "During extract", exception=e,  exc_info=True)
                print 'error in portal:', p.url
                print e


def extract(portal, dbm, snapshot, out):
    log.info("Extracting DCAT mappings from ", portals=portal.id)

    sndir=os.path.join(out, snapshot)
    if not os.path.exists(sndir):
        os.makedirs(sndir)

    pdir=os.path.join(sndir, portal.id)
    if not os.path.exists(pdir):
        os.makedirs(pdir)

    ae = SAFEAnalyserSet()
    ae.add(DCATConverter(portal))





    dcat_file=os.path.join(pdir, 'dcat.json')
    meta_file=os.path.join(pdir, 'meta.json')

    ae.add(DCATStore(dcat_file))
    ae.add(MetaStore(meta_file))

    iter = Dataset.iter(dbm.getDatasetsAsStream(portal.id, snapshot=snapshot))
    process_all( ae, iter)



    #fname='dcat'
    #with open(os.path.join(pdir, fname+ '.pkl'), 'wb') as f:
    #    pickle.dump(dcatlist.getResult(), f)
    #    print 'Writing dict to ',f
    #with open(os.path.join(pdir, fname+'.json'), 'w') as f:
    #    json.dump(dcatlist.getResult(), f)
    #    print 'Writing dict to ',f


    #fname='metadata'
    #with open(os.path.join(pdir, fname+ '.pkl'), 'wb') as f:
    #    pickle.dump(metalist.getResult(), f)
    #    print 'Writing dict to ',f
    #with open(os.path.join(pdir, fname+'.json'), 'w') as f:
    #    json.dump(metalist.getResult(), f)
    #    print 'Writing dict to ',f


class MetaStore(Analyser):
    def __init__(self, fname):
        self.f= open(file, "w")
        self.f.write("[")


    def analyse_Dataset(self, dataset):

        self.write(dataset.data)

    def done(self):
        self.f.write("]")
        self.f.close()
        log.info("Done, closing", f=self.f)


    def write(self, obj):
        """
        writes the first row, then overloads self with delimited_write
        """
        try:
            self.f.write(json.dumps(obj))
            setattr(self, "write", self.delimited_write)
        except:
            self.bad_obj(obj)

    def delimited_write(self, obj):
        """
        prefix json object with a comma
        """
        try:
            self.f.write("," + json.dumps(obj))
        except:
            log.info("BadObject", obj=obj)


class DCATStore(MetaStore):

    def __init__(self, fname):
        super(self,DCATStore).__init__(fname)


    def analyse_Dataset(self, dataset):

        if hasattr(dataset,'dcat'):
            self.write(dataset.dcat)






