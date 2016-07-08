from new.model import PortalSnapshot, PortalSnapshotQuality, Dataset
from new.web.rest.odpw_restapi_blueprint import row2dict
from odpw.new.db import DBClient, DBManager

if __name__ == '__main__':

    dbm=DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    db= DBClient(dbm)

#    D= db.getDatasetData('c7668f5ec0a1c79c6996b41b3b331047')

#    print D
#    import pprint
#    pprint.pprint(D.raw)

    for i in db.Session.query(PortalSnapshot,PortalSnapshotQuality).filter(PortalSnapshot.snapshot==1625).outerjoin(PortalSnapshotQuality, PortalSnapshot.portalid==PortalSnapshotQuality.portalid).all():
        print row2dict(i)
        #results=[row2dict(i) for i in PortalSnapshot.query.filter(PortalSnapshot.snapshot==snapshot).outerjoin(PortalSnapshotQuality, PortalSnapshot.portalid==PortalSnapshotQuality.portalid).all()]

    print db.Session.query(Dataset).filter(Dataset.snapshot==1625).filter(Dataset.portalid=='data_gv_at').count()