import structlog
from new.core.dataset_converter import dict_to_dcat
from odpw.db.dbm import PostgressDBM
from odpw.core.db import DBClient,DBManager
from odpw.core.model import Portal
from odpw.utils.timer import Timer

from utils import md5


log =structlog.get_logger()

if __name__ == '__main__':
    dbm= DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    #dbm= DBManager(user='opwu', password='0pwu', host='datamonitor-data.ai.wu.ac.at', port=5432, db='portalwatch')
    db= DBClient(dbm)


    portalid='www_europeandataportal_eu'
    snapshot=1629

    P=db.Session.query(Portal).filter(Portal.id==portalid).first()
    dbm1=PostgressDBM(user='opwu', password='0pwu', host='portalwatch.ai.wu.ac.at', port=5432, db='portalwatch')
    from odpw.db.models import Dataset as DDataset
    iter=DDataset.iter(dbm1.getDatasetsAsStream(portalID=portalid, snapshot=snapshot))
    for i, d in enumerate(iter):

        md5v=None if d.data is None else md5(d.data)

        if md5v:
            with Timer(key='dict_to_dcat'):
                #analys quality
                try:
                    if d.id=='f974bbcf-35da-4902-9d5e-8d6a2c602981':
                        d.dcat=dict_to_dcat(d.data, P)
                except Exception as e:
                    print e
                    print d.data
                    print d.id
                    break
