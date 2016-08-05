from sqlalchemy import func

from odpw.new.core.api import DBClient
from odpw.new.utils.plots import qa
from odpw.new.utils.timing import Timer
from odpw.new.core.db import DBManager, row2dict

from odpw.new.core.model import  Base, Portal, PortalSnapshot, PortalSnapshotQuality, ResourceInfo

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

    r={ 'snapshot':1632
                ,'uri':'http://'
                ,'timestamp':None
                ,'status':200
                ,'exc':'exec'
                ,'header':{}
                ,'mime':None
                ,'size':None
            }
    RI=ResourceInfo(**r)
    db.add(RI)

    q= [ i.lower() for q in qa for i,v in q['metrics'].items() ]

    import pandas as pd

    data=[]
    for R in db.Session.query(PortalSnapshot).filter(PortalSnapshot.portalid=="data_wu_ac_at"):
        data.append(row2dict(R))

    df=pd.DataFrame(data)

    #evolutionCharts(df)
