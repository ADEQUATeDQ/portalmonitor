from odpw.core.api import DBClient
from odpw.core.db import DBManager


def getLabel(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    #return "%d:%02d:%02d" % (h, m, s)
    return "%d:%02d" % (h, m)


if __name__ == '__main__':

    dbm=DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    db= DBClient(dbm)

    snapshot=1637
    q=db.getUnfetchedResources(snapshot, batch=100000)
    print str(q)
    c=0
    for r in q:
        c+=1
        if r[0]=="http://www.cso.ie/px/pxeirestat/DATABASE/Eirestat/Capital%20Stock%20of%20Fixed%20Assets/CSA02.px":
            print "AUTSCH"
        if c%10000==0:
            print c


    for valid in db.validURLDist(snapshot,portalid='data_wu_ac_at'):
        print valid

    q=db.statusCodeDist(snapshot,portalid='data_wu_ac_at')
    print str(q)
    for uri in q:
        print uri

    for RI in db.getResourceInfos(snapshot,portalid='data_wu_ac_at'):
        print RI