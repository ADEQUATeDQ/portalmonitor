import csv
import json
from urlparse import urlparse


def _calc_id(url):
    o = urlparse(url)
    id = o.netloc.replace('.','_')
    if o.path and len(o.path) > 1:
        id += o.path.replace('/','_')
    return id


def to_json(csv_file):
    result = {}
    with open(csv_file) as f:
        csvr = csv.reader(f)

        for row in csvr:
            _calc_id(row[0])

            result[_calc_id(row[0])] = {'url': row[0], 'api': row[1], 'software': row[2], 'countryCode': row[3]}

    return result


if __name__ == '__main__':
    dict = to_json(csv_file='portals.csv')
    with open('portals.json', 'wb') as f:
        json.dump(dict, f, indent=4, sort_keys=True)

