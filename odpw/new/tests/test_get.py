import time

from new.db import DBClient

from odpw.db.dbm import PostgressDBM


if __name__ == '__main__':
    db= DBClient(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    #db.init(Base)
    dbm=PostgressDBM(user='opwu', password='0pwu', host='portalwatch.ai.wu.ac.at', port=5432, db='portalwatch')







    start=time.time()
    d=[]
    for D in dbm.getDatasets(portalID='data_gv_at', snapshot=1429):

        d.append(D.id)
    print 'old',time.time()-start, len(d)

    start=time.time()
    dd=[]
    for D in db.getDatasets(portalid='data_gv_at', snapshot=1429):
        dd.append(D.id)
    print 'new',time.time()-start, len(dd)

    for ds in dd:
        if ds not in d:
            print 'missing-d',ds
    for ds in d:
        if ds not in dd:
            print 'missing-dd',ds

