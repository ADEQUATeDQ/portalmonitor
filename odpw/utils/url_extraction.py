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


def help():
    return "Extract URLs from datasets"
def name():
    return 'URLs'

def setupCLI(pa):
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument('-s','--software',choices=['CKAN', 'Socrata', 'OpenDataSoft'], dest='software')
    pa.add_argument('-o','--out',type=str, dest='out' , help="the out directory for the list of urls (and downloads)")
    pa.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")
    pa.add_argument('--store', type=bool, dest='store', default=False, help="store the files in the out directory")

def cli(args, dbm):
    if args.out is None:
        raise IOError("--out is not set")
    if args.snapshot is None:
        raise IOError("--snapshot not set")

    if not os.path.exists(args.out):
        os.makedirs(args.out)

    if args.url:
        p = dbm.getPortal(url=args.url)
        if not p:
            raise IOError(args.url + ' not found in DB')
        portals = [p]
    else:
        portals = [p for p in Portal.iter(dbm.getPortals())]

    all_urls = {}
    for p in portals:
        try:
            extract_ckan_urls(all_urls, p, args.snapshot, dbm, args.out, args.store)
        except Exception as e:
            print 'error in portal:', p.url
            print e

    with open(os.path.join(args.out, 'csv_urls_' + str(args.snapshot) + '.pkl'), 'wb') as f:
        pickle.dump(all_urls, f)


def store_file(url, filename):
    testfile = urllib.URLopener()
    testfile.retrieve(url, filename)


def valid_filename(s):
    return "".join(x for x in s if x.isalnum() or x == '.')


def extract_ckan_urls(urls, portal, snapshot, dbm, out, store_files):
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
                        name = res.get('name', url)
                        # store metadata
                        if url not in urls:
                            urls[url] = {'portal': {}}
                        urls[url]['title'] = name
                        urls[url]['format'] = format
                        if portal.id not in urls[url]['portal']:
                            urls[url]['portal'][portal.id] = []
                        urls[url]['portal'][portal.id].append(data.get('name'))

                        if store_files:
                            filename = os.path.join(path, valid_filename(url))
                            if url not in urls:
                                store_file(url, filename)
                                urls[url]['file'] = filename
                    except Exception as e:
                        print 'error loading url:', url
                        print e
