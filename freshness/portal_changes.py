from collections import defaultdict
import json
from freshness import json_compare
from odpw.db.models import Dataset
from odpw.utils.util import getSnapshot

__author__ = 'sebastian'

def name():
    return 'Changes'
def help():
    return "Get changed datasets of a portal"

def setupCLI(pa):
    pa.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")
    pa.add_argument("-sn","--snapshot",  help='what snapshot to start', dest='snapshot')


def change_history(p, sn, dbm):
    res = dbm.getSnapshotsFromPMD(portalID=p.id)

    snapshots = sorted(int(s) for _, s in res if int(s) >= sn)

    ds_dict = defaultdict(lambda: {})
    for sn in snapshots:
        datasets = Dataset.iter(dbm.getDatasets(portalID=p.id, snapshot=sn))
        for d in datasets:
            ds_dict[d.id][d.snapshot] = d.data
    return ds_dict

def cli(args,dbm):

    sn = int(getSnapshot(args))
    if not sn:
        print 'start snapshot required'
        return

    if not args.url:
        print 'url required'
        return


    p = dbm.getPortal( apiurl=args.url)
    ds_dict = change_history(p, sn, dbm)

    with open('hdx_ds.json', 'w') as f:
        json.dump(ds_dict, f)


if __name__ == '__main__':
    with open('/home/sebastian/hdx_ds.json') as f:
        data = json.load(f)
    total = 0
    count = 0
    for ds_id in data:
        total += 1
        prev_content = None
        for s in xrange(1603, 1606):
            sn = str(s)
            if sn in data[ds_id]:
                content = data[ds_id][sn]

                if prev_content:
                    diffs = json_compare.jsondiff(prev_content, content)
                    if diffs:
                        count += 1
                        print ds_id, content.get('name', '')
                        print 'snapshot', s-1, s
                        for mode, selector, changes in diffs:
                            print mode, selector, changes
                        print
                prev_content = content
    print 'total', total
    print 'changes', count