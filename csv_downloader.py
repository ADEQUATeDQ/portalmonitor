from odpw.db.models import Portal, Dataset

__author__ = 'sebastian'

import argparse
import csv
import json
import os
import sys
import urllib
import pickle
from odpw.db.dbm import PostgressDBM

csv_related_formats = ['csv', '.csv', 'tsv', '.tsv', 'application/csv', 'text/comma-separated-values', 'csv-datei',
                       'csv-semicolon delimited', 'csv-tab delimited', 'csv:1', 'text/csv',
                       'text/tab-separated-values', 'text/x-csv']

def store_file(url, filename):
    testfile = urllib.URLopener()
    testfile.retrieve(url, filename)


def valid_filename(s):
    return "".join(x for x in s if x.isalnum() or x == '.')


def load_csv_urls(urls, portal, snapshot, dbm, out):
    path = os.path.join(out, portal.id)
    if not os.path.exists(path):
        os.makedirs(path)

    for dataset in Dataset.iter(dbm.getDatasetsAsStream(portal.id, snapshot=snapshot)):
        data = dataset.data
        if data is not None and 'resources' in data:
            for res in data.get("resources"):
                format = res.get("format", '').strip().lower()
                if format in csv_related_formats:
                    url = None
                    try:
                        url = res.get("url").strip()
                        #filename = os.path.join(path, valid_filename(url))
                        #if url not in urls:
                        #    store_file(url, filename)
                        name = res.get('name', url)
                        # store metadata
                        if url not in urls:
                            urls[url] = {'portal': {}}
                        #urls[url]['file'] = filename
                        urls[url]['title'] = name
                        urls[url]['format'] = format
                        if portal.id not in urls[url]['portal']:
                            urls[url]['portal'][portal.id] = []
                        urls[url]['portal'][portal.id].append(data.get('name'))
                    except Exception as e:
                        print 'error loading url:', url
                        print e


def parseArgs(pa):
    pa.add_argument('--pId', help='Open Data catalog ID')
    pa.add_argument('--snapshot', help='Snapshot')
    pa.add_argument('--out', help='Output')
    pa.add_argument('--host', help='host', default="portalwatch.ai.wu.ac.at")
    pa.add_argument('--db', help='database name', default="portalwatch")
    pa.add_argument('--port', help='port', default=5432)

    args = pa.parse_args()
    if args.out is None:
        pa.error("--out is not set")
    if args.snapshot is None:
        pa.error("--snapshot not set")
    return args


def main(argv):
    pa = argparse.ArgumentParser()
    args = parseArgs(pa)

    if not os.path.exists(args.out):
        os.makedirs(args.out)


    dbm = PostgressDBM(args.db, args.host, args.port)
    if args.pId:
        p = dbm.getPortal(portalID=args.pId)
        if not p:
            raise IOError(args.pid + ' not found in DB')
        portals = [p]
    else:
        portals = [p for p in Portal.iter(dbm.getPortals())]

    all_urls = {}
    for p in portals:
        try:
            load_csv_urls(all_urls, p, args.snapshot, dbm, args.out)
        except Exception as e:
            print 'error in portal:', p.url
            print e

#    try:
#        with open(os.path.join(args.out, 'urls.json'), 'wb') as f:
#            json.dump(all_urls, f, ensure_ascii=False)
#    except Exception as e:
#        print 'error while writing json:'
#        print e
    with open(os.path.join(args.out, 'csv_urls_' + args.snapshot + '.pkl'), 'wb') as f:
        pickle.dump(all_urls, f)

if __name__ == "__main__":
    main(sys.argv)