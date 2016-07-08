from urlparse import urlparse

import pycountry

from odpw.new.db import DBClient, DBManager
from odpw.new.model import Portal, Base
from odpw.db.dbm import PostgressDBM


def _calc_id(url):
    o = urlparse(url)
    id = o.netloc.replace('.','_')
    if o.path and len(o.path) > 1:
        id += o.path.replace('/','_')
    return id.lower()

if __name__ == '__main__':
    #dbm= DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    dbm= DBManager(user='opwu', password='0pwu', host='datamonitor-data.ai.wu.ac.at', port=5432, db='portalwatch')
    dbm.db_DropEverything()
    dbm.init(Base)
    db= DBClient(dbm)

    dbm=PostgressDBM(user='opwu', password='0pwu', host='portalwatch.ai.wu.ac.at', port=5432, db='portalwatch')

    P = Portal(
                id=_calc_id('http://daten.buergernetz.bz.it/de/')
                ,uri='http://daten.buergernetz.bz.it/de/'
                ,apiuri = 'http://daten.buergernetz.bz.it/de/api/3'
                ,software = 'CKAN'
                ,iso = 'IT'
                ,active= True
                )
    db.add(P)
    c=0
    for p in dbm.getPortals():

        if len(p['iso3'])>0:
            iso = pycountry.countries.get(alpha3=p['iso3']).alpha2
        else:
            iso='EU'
        P = Portal(
                id=_calc_id(p['url'])
                ,uri=p['url']
                ,apiuri = p['apiurl']
                ,software = p['software']
                ,iso = iso
                ,active= True
                )
        print P

        db.add(P)
        c+=1

    print c