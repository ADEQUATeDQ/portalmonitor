'''
Created on Jun 25, 2015

@author: jumbrich
'''

from util import getSnapshot,getExceptionCode,ErrorHandler as eh
import util
import time
from pymongo import Connection
from pymongo.son_manipulator import SONManipulator
from _socket import timeout
from pymc.distributions import snapshot
import json
import hashlib
from db.models import Portal, Dataset, PortalMetaData, Resource
from db.dbm import PostgressDBM
from fetch import analyseDataset
#from db.POSTGRESManager import PostGRESManager

import logging
from sqlalchemy.exc import IntegrityError
log = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()

class KeyTransform(SONManipulator):
    """Transforms keys going to database and restores them coming out.

    This allows keys with dots in them to be used (but does break searching on
    them unless the find command also uses the transform.

    Example & test:
        # To allow `.` (dots) in keys
        import pymongo
        client = pymongo.MongoClient("mongodb://localhost")
        db = client['delete_me']
        db.add_son_manipulator(KeyTransform(".", "_dot_"))
        db['mycol'].remove()
        db['mycol'].update({'_id': 1}, {'127.0.0.1': 'localhost'}, upsert=True,
                           manipulate=True)
        print db['mycol'].find().next()
        print db['mycol'].find({'127_dot_0_dot_0_dot_1': 'localhost'}).next()

    Note: transformation could be easily extended to be more complex.
    """

    def __init__(self, replace, replacement):
        self.replace = replace
        self.replacement = replacement

    def transform_key(self, key):
        """Transform key for saving to database."""
       # print key
        return key.replace(self.replace, self.replacement)

    def revert_key(self, key):
        """Restore transformed key returning from database."""
        return key.replace(self.replacement, self.replace)

    def transform_incoming(self, son, collection):
        """Recursively replace all keys that need transforming."""
        for (key, value) in son.items():
            if self.replace in key:
                if isinstance(value, dict):
                    son[self.transform_key(key)] = self.transform_incoming(
                        son.pop(key), collection)
                elif isinstance(value, list):
                    son[self.transform_key(key)] = self.__transform_list(son.pop(key), collection)
                else:
                    son[self.transform_key(key)] = son.pop(key)
            elif isinstance(value, dict):  # recurse into sub-docs
                son[key] = self.transform_incoming(value, collection)
            elif isinstance(value, list):
                son[key] = self.__transform_list(value, collection)
        return son

    def __transform_list(self, in_list, collection):
        out_list = []
        for son in in_list:
            if isinstance(son, dict):
                out_list.append(self.transform_incoming(son, collection))
            elif isinstance(son, list):
                out_list.append(self.__transform_list(son, collection))
            else:
                out_list.append(son)
        return out_list

    def transform_outgoing(self, son, collection):
        """Recursively restore all transformed keys."""
        for (key, value) in son.items():
            if self.replacement in key:
                if isinstance(value, dict):
                    son[self.revert_key(key)] = self.transform_outgoing(
                        son.pop(key), collection)
                else:
                    son[self.revert_key(key)] = son.pop(key)
            elif isinstance(value, dict):  # recurse into sub-docs
                son[key] = self.transform_outgoing(value, collection)
        return son


#===============================================================================
# def fetchDataset(dsJSON, stats, dbm, sn, first=False):
#     props={
#         'status':-1,
#         'md5':None,
#         'data':None,
#         'exception':None
#         }
#     try:
#         props['status']=dsJSON['respCode']
# 
#         cnt= stats['fetch_stats']['respCodes'].get(dsJSON['respCode'],0)
#         stats['fetch_stats']['respCodes'][dsJSON['respCode']]= (cnt+1)
# 
#         if dsJSON['respCode'] == 200:
#             data = dsJSON['content']
#             
#             d = json.dumps(data, sort_keys=True, ensure_ascii=True)
#             data_md5 = hashlib.md5(d).hexdigest()
#             props['md5']=data_md5
#             props['data']=data
# 
#             stats=extract_keys(data, stats)
# 
#             if 'resources' in data:
#                     stats['res'].append(len(data['resources']))
#                     for resJson in data['resources']:
#                         stats['res_stats']['total']+=1
#                         
#                         tR =  Resource.newInstance(url=resJson['url'], snapshot=snapshot)
#                         R = dbm.getResource(tR)
#                         if not R:
#                             #do the lookup
#                             R = Resource.newInstance(url=resJson['url'], snapshot=snapshot)
#                             try:
#                                 dbm.insertResource(R)
#                             except Exception as e:
#                                 print e, resJson['url'],'-',snapshot
# 
#                         R.updateOrigin(pid=portal.id, did=dataset.dataset)
#                         dbm.updateResource(R)
# 
#             dbm.updateDataset(dataset)
# 
# 
#     except Exception as e:
#         #eh.handleError(log,'fetching dataset information', exception=e,pid=stats['portal'].id,
#         #          apiurl=stats['portal'].apiurl,
#         #          exc_info=True)
#         props['status']=util.getExceptionCode(e)
#         props['exception']=str(type(e))+":"+str(e.message)
# 
#     
#     d = Dataset(snapshot=sn,portal=stats['portalID'],dataset=dsJSON['id'], **props)
#     
#     dbm.updateDataset(d)
#===============================================================================

def extract_keys(data, stats):

    core=stats['general_stats']['keys']['core']
    extra=stats['general_stats']['keys']['extra']
    res=stats['general_stats']['keys']['res']

    for key in data.keys():
        if key == 'resources':
            for r in data['resources']:
                for k in r.keys():
                    if k not in res:
                        res.append(k)
        elif key == 'extras' and isinstance(data['extras'],dict):
            for k in data['extras'].keys():
                if k not in extra:
                    extra.append(k)
        else:
            if key not in core:
                core.append(key)

    return stats


def fetching(obj):
    portal = obj['portal']
    sn=obj['sn']
    dbm=obj['dbm']
    fullfetch=obj['fullfetch']
    
    
    P = Portal.newInstance(url=portal['pURL'],apiurl=portal['url'])
    P = dbm.getPortal(portalID=P.id)
    if not P: 
        P = Portal.newInstance(url=portal['pURL'],apiurl=portal['url'])
        dbm.insertPortal(P)
        print "Inserting new Portal"
    
    stats={
        'portal':portal,
        'datasets':-1, 'resources':-1,
        'status':-1,
        'fetch_stats':{'respCodes':{}, 'fullfetch':fullfetch},
        'general_stats':{
            'keys':{'core':[],'extra':[],'res':[]}
        },
        'res_stats':{'respCodes':{},'total':0, 'resList':[]}
    }
    stats['res']=[]
    
    pmd = dbm.getPortalMetaData(portalID=P.id, snapshot=sn)
    if not pmd:
        pmd = PortalMetaData(portal=P.id, snapshot=sn)
        dbm.insertPortalMetaData(pmd)
        
    total = db[portal['id']].find({'time': snapshot}).count()
    if pmd.datasets == total:
        print '    Portal',id,'for', sn,'already migrated', pmd.datasets, total
        return 

    if not pmd.fetch_stats:
        pmd.fetch_stats={}                
    pmd.fetch_stats['fetch_start']=obj['sn-time'].isoformat()
    dbm.updatePortalMetaData(pmd)
            
    try:
        if fullfetch:
            #fetch the dataset descriptions
            
            
            
            c=0;
            steps=total/10
            start = time.time()
            interim = time.time()
            
            print "... migrating ", sn, 'with', total, 'datasts', 'currently',pmd.datasets
            for ds in db[portal['id']].find({'time': snapshot}, timeout=False):
                c+=1
                if stats['datasets'] ==-1:
                    stats['datasets']=1
                else:
                    stats['datasets']+=1
                
                status=ds['respCode']
                data = ds['content']
                entity=ds['id']
                props={
                        'status':-1,
                        'md5':None,
                        'data':None,
                        'exception':None
                        }
                try:
                    stats['portal']=P
                    props=analyseDataset(data, entity, stats, dbm, sn,status)
                    
                except Exception as e:
                    print e
                    eh.handleError(log,'fetching dataset information', exception=e,pid=stats['portalID'],
                                   url=Portal['url'],
                                   exc_info=True)
                    props['status']=util.getExceptionCode(e)
                    props['exception']=str(type(e))+":"+str(e.message)
                
                d = Dataset(snapshot=sn,portal=P.id, dataset=entity, **props)
                try:
                    dbm.insertDatasetFetch(d)
                except IntegrityError as e:
                    pass
                      
                
                if c%steps == 0:
                    elapsed = (time.time() - start)
                    interim = (time.time() - interim)
                    util.progressINdicator(c, total, elapsed=elapsed, interim=interim)
                    interim = time.time()

                
            #if we had results
            if stats['datasets']>0: 
                stats['status']=200
            else:
                stats['status']=911
            
            
            stats['resources']=sum(stats['res'])

    except Exception as e:
        print e
        eh.handleError(log,"fetching dataset information", exception=e, apiurl=P.apiurl,exc_info=True)
        #log.exception('fetching dataset information', apiurl=Portal.apiurl,  exc_info=True)
        
    try:
        pmd.updateStats(stats)
        ##UPDATE
        #   ds-fetch statistics
        dbm.updatePortalMetaData(pmd)
    except Exception as e:
        #eh.handleError(log,'Updating DB',exception=e,pid=Portal.id, exc_info=True)
        #log.critical('Updating DB', pid=Portal.id, exctype=type(e), excmsg=e.message,exc_info=True)
        pass

    return stats



from pprint import pprint
import datetime

if __name__ == '__main__':
    logging.basicConfig()
    con = Connection("137.208.51.23", 27017)
    db = con["odwu"]
    db.add_son_manipulator(KeyTransform(".", "_dot_"))
    dbm= PostgressDBM(host="bandersnatch.ai.wu.ac.at")
    #dbm = PostGRESManager(host="bandersnatch.ai.wu.ac.at")
    
    portals=[]
    for p in db['odp'].find(timeout=False):
        portals.append(p)
    
    processed=[
               #================================================================
               # 'http://datacatalogs.org/',
               # 'http://data.yokohamaopendata.jp',
                'http://dati.gov.it/',
               # 'http://data.gov.au',
               # 'http://data.graz.gv.at/',
               # 'http://data.gv.at',
               # 'http://data.hdx.rwlabs.org',
               # 'http://datahub.io/',
               # 'http://data.kk.dk/',
               # 'http://data.lexingtonky.gov/',
               # 'http://datameti.go.jp/data/',
               # 'http://data.nsw.gov.au',
               # 'http://data.ohouston.org/',
               # 'http://data.ottawa.ca/',
               # 'http://data.qld.gov.au',
               # 'http://data.rio.rj.gov.br/',
               # 'http://data.sa.gov.au/',
               # 'http://data.ug/',
               # 'http://dati.toscana.it/',
               # 'http://datosabiertos.malaga.eu/',
               # 'http://datos.codeandomexico.org/',
               # 'http://donnees.ville.montreal.qc.ca/',
               # 'http://gisdata.mn.gov',
               # 'http://hubofdata.ru/',
               # 'http://www.openumea.se/',
               # 'http://daten.hamburg.de/'
               #================================================================
               
               
               ]
    print len(portals)
    for portal in portals:
        if 'pURL' in portal and portal['pURL'] not in processed:
            print "Migrating", portal['pURL'],"with",len(portal['snapshots']), 'snapshots'
            for snapshot in portal['snapshots']:
                date=datetime.datetime.fromtimestamp(snapshot)
                y=date.isocalendar()[0]
                w=date.isocalendar()[1]
                sn=str(y)+'-'+str(w)
             
                
                
                obj={
                      'portal':portal,
                      'sn':sn,
                      'sn-time':date,
                      'dbm':dbm,
                      'fullfetch':True,
                      'mdb':db
                      }
                print "  -> (", snapshot,')', date, ' ->',sn 
                fetching(obj)
                
            
        
    