from copy import copy
import json

__author__ = 'sebastian'

from dictdiffer import diff, dot_lookup


def exists(selector, json):
    return _exists(copy(selector), json)
def _exists(s, j):
    if len(s) == 0:
        return True
    k = s.pop(0)
    if k in j:
        return _exists(s, j[k])
    if isinstance(j, list) and len(j) > k:
        return _exists(s, j[k])
    return False

def select(selector, json):
    return _select(copy(selector), json)
def _select(s, j):
    if len(s) == 0:
        return j
    k = s.pop(0)
    return _select(s, j[k])


def jsondiff(j1, j2):
    diffs = diff(j1, j2)
    for mode, selector, changes in diffs:
        print mode, selector, changes
        if exists(selector, j1):
            v = select(selector, j1)
            print v
            query = '.'.join(unicode(s) for s in selector)
            v = dot_lookup(j1, query)
            print v
        print


if __name__ == '__main__':
    j1 = json.load(open('testcases/dataset.json'))
    j2 = json.load(open('testcases/dataset_mod_2.json'))
    jsondiff(j1, j2)