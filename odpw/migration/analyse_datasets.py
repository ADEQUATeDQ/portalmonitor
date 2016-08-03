from odpw.new.core.db import DBClient
from odpw.db.dbm import PostgressDBM

if __name__ == '__main__':
    dbm=PostgressDBM(user='opwu', password='0pwu', host='portalwatch.ai.wu.ac.at', port=5432, db='portalwatch')

    db=DBClient(user='opwu', password='0pwu', host='portalwatch.ai.wu.ac.at', port=5432, db='portalwatch')

    P = db.getPortal()


    for d in dbm.getDatasets(portalID='data_wu_ac_at'):
