'''
Created on Jun 25, 2015

@author: jumbrich
'''


import util

from pymongo import Connection
from pymongo.son_manipulator import SONManipulator
from _socket import timeout
from pymc.distributions import snapshot
import json
import hashlib
from db.models import Portal, Dataset, PortalMetaData, Resource
from db.POSTGRESManager import PostGRESManager

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


def fetchDataset(dsJSON, stats, dbm, sn, first=False):
    props={
        'status':-1,
        'md5':None,
        'data':None,
        'exception':None
        }
    try:
        props['status']=dsJSON['respCode']

        cnt= stats['fetch_stats']['respCodes'].get(dsJSON['respCode'],0)
        stats['fetch_stats']['respCodes'][dsJSON['respCode']]= (cnt+1)

        if dsJSON['respCode'] == 200:
            data = dsJSON['content']
            
            d = json.dumps(data, sort_keys=True, ensure_ascii=True)
            data_md5 = hashlib.md5(d).hexdigest()
            props['md5']=data_md5
            props['data']=data

            stats=extract_keys(data, stats)

            if 'resources' in data:
                stats['res'].append(len(data['resources']))
                lastDomain=None
                for resJson in data['resources']:
                    stats['res_stats']['total']+=1

                    R = dbm.getResource(url=resJson['url'], snapshot=sn)
                    if not R:
                        #do the lookup
                        R = Resource(url=resJson['url'], snapshot=sn)
                        

                    R.updateOrigin(pid=stats['portal'].id, did=dsJSON['id'])
                    dbm.upsertResource(R)

                    cnt= stats['res_stats']['respCodes'].get(R.status,0)
                    stats['res_stats']['respCodes'][R.status]= (cnt+1)

                    if R.url not in stats['res_stats']['resList']:
                        stats['res_stats']['resList'].append(R.url)


    except Exception as e:
        #eh.handleError(log,'fetching dataset information', exception=e,pid=stats['portal'].id,
        #          apiurl=stats['portal'].apiurl,
        #          exc_info=True)
        props['status']=util.getExceptionCode(e)
        props['exception']=str(type(e))+":"+str(e.message)

    d = Dataset(snapshot=sn,portal=stats['portalID'],dataset=dsJSON['id'], **props)
    dbm.upsertDatasetFetch(d)

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
    Portal = obj['portal']
    sn=obj['sn']
    dbm=obj['dbm']
    fullfetch=obj['fullfetch']

    #log.info("Fetching", pid=Portal.id, sn=sn, fullfetch=fullfetch)
    id=util.computeID(Portal['pURL'])
    stats={
        'portal':Portal,'portalID':id,
        'datasets':-1, 'resources':-1,
        'status':-1,
        'fetch_stats':{'respCodes':{}, 'fullfetch':fullfetch},
        'general_stats':{
            'keys':{'core':[],'extra':[],'res':[]}
        },
        'res_stats':{'respCodes':{},'total':0, 'resList':[]}
    }
    stats['res']=[]
    
    pmd = PortalMetaData(portal=id, snapshot=sn)
    pmd.fetch_stats['fetch_start']=obj['sn-time'] 
            
    dbm.upsertPortalMetaData(pmd)
            
    try:
        if fullfetch:
            #fetch the dataset descriptions
            
            print "  -> ", snapshot,date, sn 
            for ds in db[portal['id']].find({'time': snapshot}, timeout=False):
                if stats['datasets']==-1:
                    stats['datasets']=1
                else:
                    stats['datasets']+=1
                
                fetchDataset(ds, stats, dbm, sn, first=False)    
                
                
            #if we had results
            if stats['datasets']>0: 
                stats['status']=200
            else:
                stats['status']=911
            
            
            stats['resources']=sum(stats['res'])

    except Exception as e:
        #eh.handleError(log,"fetching dataset information", exception=e, apiurl=Portal.apiurl,exc_info=True)
        #log.exception('fetching dataset information', apiurl=Portal.apiurl,  exc_info=True)
        pass
    try:
        pmd.updateFetchStats(stats)
        ##UPDATE
        #   ds-fetch statistics
        dbm.upsertPortalMetaData(pmd)
    except Exception as e:
        #eh.handleError(log,'Updating DB',exception=e,pid=Portal.id, exc_info=True)
        #log.critical('Updating DB', pid=Portal.id, exctype=type(e), excmsg=e.message,exc_info=True)
        pass

    return stats



from pprint import pprint
import datetime
if __name__ == '__main__':
    con = Connection("137.208.51.23", 27017)
    db = con["odwu"]
    db.add_son_manipulator(KeyTransform(".", "_dot_"))
    
    dbm = PostGRESManager(host="bandersnatch.ai.wu.ac.at")
    
    portals=[]
    for p in db['odp'].find(timeout=False):
        portals.append(p)
    
    processed=[
               'http://datacatalogs.org/',
               'http://data.yokohamaopendata.jp',
               'http://dati.gov.it/',
               'http://data.gov.au',
               'http://data.graz.gv.at/',
               'http://data.gv.at',
               'http://data.hdx.rwlabs.org',
               'http://datahub.io/',
               'http://data.kk.dk/',
               'http://data.lexingtonky.gov/',
               'http://datameti.go.jp/data/',
               'http://data.nsw.gov.au',
               'http://data.ohouston.org/',
               'http://data.ottawa.ca/',
               'http://data.qld.gov.au',
               'http://data.rio.rj.gov.br/',
               'http://data.sa.gov.au/',
               'http://data.ug/',
               'http://dati.toscana.it/',
               'http://datosabiertos.malaga.eu/',
               'http://datos.codeandomexico.org/',
               'http://donnees.ville.montreal.qc.ca/',
               'http://gisdata.mn.gov',
               'http://hubofdata.ru/',
               'http://www.openumea.se/',
               'http://daten.hamburg.de/'
               
               
               ]
    
    for portal in portals:
        print "Portal: ", portal['pURL']
        
        if portal['pURL'] in processed:
            continue
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
            
            fetching(obj)
                
            
        
    