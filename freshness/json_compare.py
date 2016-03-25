from copy import copy
import json

__author__ = 'sebastian'

from dictdiffer import diff, dot_lookup


def exists(s, j):
    if len(s) == 0:
        return True
    k = s[0]
    s = s[1:]
    if k in j:
        return exists(s, j[k])
    if k == '*':
        if isinstance(j, dict):
            return any([exists(s, j[i]) for i in j])
        if isinstance(j, list):
            return any([exists(s, j[i]) for i in range(len(j))])
    if isinstance(j, list) and len(j) > k:
        return exists(s, j[k])
    return False

def select(s, j):
    if len(s) == 0:
        return j
    k = s[0]
    s = s[1:]
    if k == '*':
        if isinstance(j, dict):
            return [select(s, j[i]) for i in j]
        if isinstance(j, list):
            return [select(s, j[i]) for i in range(len(j))]
    return select(s, j[k])


def jsondiff(j1, j2, filterMode=None, withoutKey=None, filterKey=None):
    diffs = diff(j1, j2)
    tmp = []
    for mode, selector, changes in diffs:
        # parse selector to list
        if isinstance(selector, basestring):
            selector = selector.split('.')

        # filter
        if not filterMode or mode == filterMode:
            if not withoutKey or withoutKey not in selector:
                if not filterKey or filterKey in selector:
                    tmp.append((mode, selector, changes))
    return tmp

if __name__ == '__main__':
<<<<<<< HEAD
    j1 = json.load(open('/Users/jumbrich/Dev/odpw/stats/1551/data_wu_ac_at.json'))
    j2 = json.load(open('/Users/jumbrich/Dev/odpw/stats/1603/data_wu_ac_at.json'))


    for mode, selector, changes in jsondiff(j1, j2):

        if all([ k not in selector for k in ['DCATDatasetAge','DCATResourceInDSAge','fetch_process','DatasetChangeCountAnalyser']]):
            if 'total' != selector[-1]:
                print mode, selector, changes
=======
    j1 = json.load(open('testcases/dataset.json'))
    j2 = json.load(open('testcases/dataset_mod_2.json'))
    print jsondiff(j1, j2)
>>>>>>> d18579cce2b359f22b6bce56926093dbb93bd5e0
