from new.core.db import DBClient, DBManager




def getLabel(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    #return "%d:%02d:%02d" % (h, m, s)
    return "%d:%02d" % (h, m)


if __name__ == '__main__':

    dbm=DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    db= DBClient(dbm)



    for valid in db.validURLDist(1629,portalid='data_wu_ac_at'):
        print valid

    for uri in db.statusCodeDist(1629,portalid='data_wu_ac_at'):
        print uri

    for RI in db.getResourceInfos(1629,portalid='data_wu_ac_at'):
        print RI