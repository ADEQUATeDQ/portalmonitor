import json
from dateutil.parser import parse

__author__ = 'sebastian'

import os
import json_compare

TS_QUERY = ['query', 'pages', '*', 'revisions', '*', 'timestamp']
TITLE_QUERY = ['query', 'pages', '*', 'title']



if __name__ == '__main__':
    #COLLECTED REVISIONS
    all_revs = {}

    revs='revs'
    for (dirpath, dirnames, filenames) in os.walk(revs):

        for file in filenames:
            fname = os.path.join(dirpath, file)
            if os.path.isfile(fname):
                with open(fname) as f:
                    j = json.load(f)

                t = json_compare.select(TITLE_QUERY, j)[0]
                if json_compare.exists(TS_QUERY, j):
                    all_revs[t] = json_compare.select(TS_QUERY, j)

    with open('ts.json', 'w') as f:
        json.dump(all_revs, f, indent=4)

