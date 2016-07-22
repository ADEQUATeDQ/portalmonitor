from new.utils.timing import Timer
from odpw.new.core.db import DBClient, DBManager

from new.core.model import  Base, Portal, PortalSnapshot

import structlog
log =structlog.get_logger()

def getLabel(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    #return "%d:%02d:%02d" % (h, m, s)
    return "%d:%02d" % (h, m)


if __name__ == '__main__':

    dbm=DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    #dbm.db_DropEverything()
    #dbm.init(Base)

    db= DBClient(dbm)

    with Timer(verbose=True):
        r=db.Session.query(Portal,  Portal.last_snapshot,Portal.first_snapshot,Portal.snapshot_count)
        print str(r)
        for i in r:
            s=db.Session.query(PortalSnapshot).filter(PortalSnapshot.snapshot==i[1]).filter(PortalSnapshot.portalid==i[0].id)
            for ii in s:
                print ii
            print i

    #r=db.Session.query(PortalSnapshot).filter(PortalSnapshot.snapshot==1628)
    #for i in r:
    #    print i



    portalid='data_wu_ac_at'
    snapshot=1629
    for r in db.organisationDist(snapshot,portalid):
        print r

    for r in db.licenseDist(snapshot,portalid):
        print r