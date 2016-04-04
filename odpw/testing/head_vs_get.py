from odpw.utils.timer import Timer

__author__ = 'sebastian'


import requests

url = 'https://data.cityofchicago.org/api/views/y93d-d9e3/rows.xls?accessType=DOWNLOAD'
url= 'https://healthdata.nj.gov/api/views/ye5m-96tk/rows.json?accessType=DOWNLOAD'

def head(url):
    r1 = requests.head(url=url,timeout=(0.5, 1.0), allow_redirects=True)
    return r1.headers

def get(url):
    r = requests.get(url, stream=True,headers={"range": "bytes=3-5",'Accept-Encoding': 'identity'})
    for chunk in r.iter_content(1024):
        break
    r.close()
    return r.headers




with Timer(verbose=True) as t:
    print head(url)

with Timer(verbose=True) as t:
    print get(url)