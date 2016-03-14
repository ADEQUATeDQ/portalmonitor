from collections import defaultdict
import json
from odpw.db.models import Dataset, Portal
from odpw.utils.util import getSnapshot

__author__ = 'sebastian'


def name():
    return 'Freshness'
def help():
    return "Freshness of data portal"

def setupCLI(pa):
    gfilter = pa.add_argument_group('filters', 'filter option')
    gfilter.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")
    gfilter.add_argument("--software",  help='filter software', dest='software')

    pa.add_argument("-s","--start",  help='what start snapshot', dest='start')
    pa.add_argument("-e","--end",  help='what end snapshot', dest='end')



def get_res_with_modified(datasets, snapshot, data_dict):
    for dataset in datasets:
        if 'resources' in dataset.data and isinstance(dataset.data['resources'], list):
            resources = dataset.data['resources']
            for r in resources:
                if 'url' in r and 'last_modified' in r and r['last_modified'] is not None:
                    data_dict[r['url']]['value'].append(r['last_modified'])
                    data_dict[r['url']]['snapshots'].append(snapshot)



def get_socrata_modified(datasets, snapshot, data_dict):
    for dataset in datasets:
        if 'view' in dataset.data:
            content = dataset.data['view']
        else:
            content = dataset.data
        if 'id' in content and 'rowsUpdatedAt' in content and content['rowsUpdatedAt'] is not None:
            data_dict[content['id']]['value'].append(content['rowsUpdatedAt'])
            data_dict[content['id']]['snapshots'].append(snapshot)



def cli(args,dbm):

    start = int(args.start)
    end = int(args.end)

    portals = []
    if args.url:
        p = dbm.getPortal( apiurl=args.url)
        if p:
            portals.append(p.id)
    elif args.software:
        ps = dbm.getPortals(software=args.software)
        for p in Portal.iter(ps):
            portals.append(p.id)

    for p_id in portals:
        res_last_modifed = defaultdict(lambda: {'snapshots': [], 'value': []})
        snapshots = sorted([s['snapshot'] for s in dbm.getSnapshotsFromPMD(portalID=p_id)])

        for past_sn in snapshots:
            if start < past_sn <= end:
                ds = dbm.getDatasetsAsStream(portalID=p_id, snapshot=past_sn)
                #get_res_with_modified(Dataset.iter(ds), snapshot=past_sn, data_dict=res_last_modifed)
                get_socrata_modified(Dataset.iter(ds), snapshot=past_sn, data_dict=res_last_modifed)

        for k, v in res_last_modifed.items():
            if end not in v['snapshots']:
                del res_last_modifed[k]

        with open('/home/sebastian/Repositories/ODPortalWatch_2/freshness/socrata/'+p_id+'_lastmodified.json', 'w') as f:
            json.dump(res_last_modifed, f)