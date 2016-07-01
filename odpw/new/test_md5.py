


import hashlib
import json

def md5(data):
    data_md5 = hashlib.md5(json.dumps(data, sort_keys=True)).hexdigest()

    return data_md5

def md52(data):
    d = json.dumps(data, sort_keys=True, ensure_ascii=True)
    data_md5 = hashlib.md5(d).hexdigest()
    return data_md5


if __name__ == '__main__':
    data = ['only', 'lists', [1,2,3], 'dictionaries', {'a':0,'b':1}, 'numbers', 47, 'strings']
    data1 = ['only', 'lists', [1,2,3], 'dictionaries', {'b':1,'a':0}, 'numbers', 47, 'strings']
    print md5(data), md52(data), md5(data)== md52(data)
    print md5(data1), md52(data1), md5(data1)== md52(data1)

