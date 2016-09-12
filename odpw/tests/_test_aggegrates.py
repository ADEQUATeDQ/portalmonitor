from core.api import DBClient
from core.db import DBManager
from odpw.services.aggregates import aggregatePortalQuality
from utils.plots import qualityChart

if __name__ == '__main__':
    dbm=DBManager(user='adequatecli', password='4dequat3', host='localhost', port=5433, db='adequate')
    #dbm=DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')

    #dbm= DBManager(user='opwu', password='0pwu', host='datamonitor-data.ai.wu.ac.at', port=5432, db='portalwatch')
    db= DBClient(dbm)
    snapshot=1636
    #aggregate(db,snapshot)



    portalid='data_gv_at'

    PSQ= aggregatePortalQuality(db, portalid,snapshot)
    #print PSQ.exac

    q=db.portalSnapshotQuality(portalid,snapshot)
    for r in q:
        print r.exac

    df=db.portalSnapshotQualityDF(portalid,snapshot)
    p= qualityChart(df)

    #
    # import pandas as pd
    # df = pd.DataFrame([{'a':True,'b':False,'c':None},{'a':False,'b':None,'c':True},{'a':True,'b':False,'c':None},{'a':True,'b':False,'c':None},{'a':True,'b':False,'c':True}])
    # print df
    # print df.info()
    # #df=df.replace(True,1)
    # #df=df.replace(False,0)
    # #print df
    # df['a']=df['a'].apply(bool).astype(int)
    # for i in ['a','b','c']:
    #     print df[i].dtype,  type(df[i].dtype)
    #     if isinstance(df[i].dtype, bool):
    #         df[i]=df[i].apply(bool).astype(int)
    #     else:
    #         df[i]=df[i].replace(True,1)
    #         df[i]=df[i].replace(False,0)
    #
    # print '-'*10
    # print df['a'].dtype
    # print '-'*10
    # print df
    # print df.info()
    # print df.describe()
