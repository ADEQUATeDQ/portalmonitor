'''
Created on Aug 12, 2015

@author: jumbrich
'''

import structlog
from odpw.utils.util import ErrorHandler
from odpw.db.models import Dataset, Portal

import odpw.utils.util as odpwutil

def fetchDataset(Portal, did, dbm):
    props={
                           'status':-1,
                           'data':None,
                           'exception':None,
                            'software':'CKAN'
                           }
    try:
        resp, status = odpwutil.getPackage(P.apiurl, id)
        props['status']=status
        if resp:
            data = resp
            odpwutil.extras_to_dict(data)
            props['data']=data
    except Exception as e:
        ErrorHandler.handleError(log,'FetchDataset', exception=e,pid=P.id, did=did,
           exc_info=True)
        props['status']=odpwutil.getExceptionCode(e)
        props['exception']=odpwutil.getExceptionString(e)

    d = Dataset(snapshot=1533, portalID=P.id, did=did, **props)

    dbm.updateDataset(d)


log =structlog.get_logger()
if __name__ == '__main__':
    from odpw.db.dbm import PostgressDBM
    dbm = PostgressDBM(host='localhost', port=5432)
    portals ={}
    for P in Portal.iter(dbm.getPortals(software='CKAN')):
        portals[P.id]=P

    for d in Dataset.iter(dbm.getDatasets(snapshot=1533, software='CKAN', status=404)):
        try:
            id=d.id
            portal_id=d.portal_id
            P= portals[portal_id]
        
            fetchDataset(P, id, dbm)
        except Exception as e:
            ErrorHandler.handleError(log,'MissingExtra', exception=e, 
                                 exc_info=True)

    for row in dbm.getMissingExtras(snapshot=1533, software='CKAN'):
        try:
            id=row['id']
            portal_id=row['portal_id']
            P= portals[portal_id]
            
            fetchDataset(P, id, dbm)
        except Exception as e:
            ErrorHandler.handleError(log,'MissingExtra', exception=e, 
                                     exc_info=True)
