
__author__ = 'jumbrich'

from sqlalchemy.engine.result import RowProxy

import urlnorm
import urllib
import odpw.utils.util as util
from odpw.utils.util import ErrorHandler as eh, getSnapshotfromTime,\
    getPreviousWeek, getNextWeek

from datetime import datetime

import structlog
log =structlog.get_logger()
import json

from pprint import pformat

class Model(object):
    
    def __init__(self, **kwargs):
        self.__hash= hash(pformat(kwargs)) 
    
    def __eq__(self, other):
        if isinstance(other, Model):
            return (self.__hash == other.__hash )
        else:
            return False
    
    def __ne__(self, other):
        return (not self.__eq__(other))
    
    def __hash__(self, *args, **kwargs):
        return self.__hash

class Portal(Model):

    @classmethod
    def iter(cls, iterable):
        for i in iterable:
            r = Portal.fromResult(dict(i))
            yield r
        return
        
    @classmethod
    def fromResult(cls, result):
        if isinstance(result, RowProxy):
            result = dict(result)
        if not isinstance(result, dict):
            return None
        
        url = result['url']
        apiurl = result['apiurl']
        iso3 = result['iso3']
        pid = result['id']
        software =result['software']

        return cls(pid, url, apiurl, software, iso3)

    @classmethod
    def newInstance(cls, pid, url, apiurl, software, iso3):
        p = cls( pid,url, apiurl, software, iso3 )
        log.info("new portal instance", pid=p.id, apiurl=p.apiurl, software=software, iso3=iso3)
        return p


    def __init__(self, pid, url, apiurl, software, iso3):
        super(Portal,self).__init__(**{'url':url,'apiurl':apiurl})
        
        self.id = pid
        self.url = url
        self.apiurl = apiurl
        self.software = software
        self.iso3 = iso3

class DatasetMetaData(Model):

    @classmethod
    def iter(cls, iterable):
        for i in iterable:
            r = DatasetMetaData.fromResult(dict(i))
            yield r
        return

    @classmethod
    def fromResult(cls, result):
        if isinstance(result, RowProxy):
            result = dict(result)

        snapshot = result['snapshot']
        portal_id = result['portal_id']
        did = result['id']
        dcat = result['dcat']
        ckan = result['ckan']

        return cls(snapshot=snapshot, portalID=portal_id, did=did, dcat=dcat, ckan=ckan)

    def __init__(self, did, portalID, snapshot, dmd):
        super(DatasetMetaData,self).__init__(**{'snapshot':snapshot,'portal_id':portalID,'id':did})

        self.snapshot = snapshot
        self.portal_id = portalID
        self.id = did
        self.dcat = dmd['dcat'] if 'dcat' in dmd else None
        self.ckan = dmd['ckan'] if 'ckan' in dmd else None


class Dataset(Model):
    
    @classmethod
    def iter(cls, iterable):
        for i in iterable:
            r = Dataset.fromResult(dict(i))
            yield r
        return

    @classmethod
    def fromResult(cls, result):
        if isinstance(result, RowProxy):
            result = dict(result)

        snapshot = result['snapshot']
        portal_id = result['portal_id']
        did = result['id']

        del result['portal_id']
        del result['id']
        del result['snapshot']

        for i in ['data', 'qa_stats']:
            if isinstance(result[i], unicode):
                result[i] = json.loads(result[i])

        return cls(snapshot=snapshot, portalID=portal_id, did=did, **result)

    def __init__(self, snapshot, portalID, did, data=None, **kwargs):
        super(Dataset,self).__init__(**{'snapshot':snapshot,'portal_id':portalID,'id':did})

        self.snapshot = snapshot
        self.portal_id = portalID
        self.id = did

        self.data = data
        self.status = None
        self.exception = None
        self.md5 = None
        self.change = None
        self.qa_stats = None
        self.software= None

        for key, value in kwargs.items():
            setattr(self, key, value)



class DatasetLife(Model):
    
    @classmethod
    def iter(cls, iterable):
        for i in iterable:
            r = DatasetLife.fromResult(dict(i))
            yield r
        return

    @classmethod
    def fromResult(cls, result):
        if isinstance(result, RowProxy):
            result = dict(result)

        portal_id = result['portal_id']
        did = result['id']

        del result['portal_id']
        del result['id']
        
        return cls(portalID=portal_id, did=did, **result)

    def __init__(self,  portalID, did, data=None, **kwargs):
        super(DatasetLife,self).__init__(**{'portal_id':portalID,'id':did})
        
        self.portal_id = portalID
        self.id = did

        self.snapshots= None

        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def updateSnapshot(self, created,sn ):
        c_sn=getSnapshotfromTime(created)
        if self.snapshots is None:
            self.snapshots={'created':[c_sn], 'indexed':[(sn,sn)]}
        else:
            if c_sn not in self.snapshots['created']:
                self.snapshots['created'].append(c_sn)
            
            sns=self.snapshots['indexed']
            #sort sn tuples by start date
            sns=sorted(sns, key=lambda tup: tup[0])
            
            min = sns[0][0]
            max= sns[-1][1]
            
            new =[]
            
            for t in sns:
                if sn < t[0]: #ds_sn is before earliest tuple
                    if sn == getPreviousWeek(t[0]): #just one week before, 
                        t[0] = sn
                    elif min == t[0]: # more then one week before
                        new.append((sn,sn))
                elif sn > t[1]:
                    if sn == getNextWeek(t[1]):
                        t[1] = sn
                    elif max == t[1]: # more then one week before
                        new.append((sn,sn))
            
            for t in new:
                self.snapshots['indexed'].append(t)
            

class PortalMetaData(Model):

    @classmethod
    def iter(cls, iterable):
        for i in iterable:
            r = PortalMetaData.fromResult(dict(i))
            yield r
        return

                             
                             


    @classmethod
    def fromResult(cls, result):
        if isinstance(result, RowProxy):
            result = dict(result)
        if not isinstance(result, dict):
            return None
        
        portal_id = result['portal_id']
        snapshot = result['snapshot']
        del result['portal_id']
        del result['snapshot']


        for i in ['res_stats', 'qa_stats', 'general_stats', 'fetch_stats']:
            if i in result and isinstance(result[i], unicode):
                result[i] = json.loads(result[i])

        return cls(portalID=portal_id,
                   snapshot=snapshot, **result)

    def __init__(self, portalID=None, snapshot=None, **kwargs):
        super(PortalMetaData,self).__init__(**{'snapshot':snapshot,'portal_id':portalID})
        self.snapshot = snapshot
        self.portal_id = portalID
        self.res_stats = None
        self.qa_stats = None
        self.fetch_stats = None
        self.general_stats = None
        self.resources = -1
        self.datasets = -1
        
        for key, value in kwargs.items():
            setattr(self, key, value)

    def fetchstart(self):
        if not self.fetch_stats:
            self.fetch_stats={}
        self.fetch_stats['fetch_start'] = datetime.now().isoformat()
        
    def headstart(self):
        if not self.res_stats:
            self.res_stats={}
        self.res_stats['head_start'] = datetime.now().isoformat()
        
    def headend(self):
        if not self.res_stats:
            self.res_stats={}
        self.res_stats['head_stop'] = datetime.now().isoformat()
        
    def fetchend(self):
        if not self.fetch_stats:
            self.fetch_stats={}
        self.fetch_stats['fetch_end'] = datetime.now().isoformat()

    def fetchTimeout(self, timeout):
        if not self.fetch_stats:
            self.fetch_stats={}
        self.fetch_stats['fetch_timeout'] = timeout

    def updateStats(self, stats):
        if 'fetch_stats' in stats:
            if not self.fetch_stats:
                self.fetch_stats = {}
            self.fetch_stats['fetch_end'] = datetime.now().isoformat()
            self.fetch_stats['fullfetch'] = stats['fetch_stats']['fullfetch']
            self.fetch_stats['respCodes'] = stats['fetch_stats']['respCodes']
            self.fetch_stats['datasets'] = stats['datasets']
            self.fetch_stats['portal_status'] = stats['status']

        if 'qa_stats' in stats:
            if not self.qa_stats:
                self.qa_stats = {}
            for key in stats['qa_stats'].keys():
                self.qa_stats[key] = stats['qa_stats'][key]
        

        if 'general_stats' in stats:
            if not self.general_stats:
                self.general_stats = {}
            for key in stats['general_stats'].keys():
                self.general_stats[key] = stats['general_stats'][key]
        
        if 'res_stats' in stats:
            if not self.res_stats:
                self.res_stats = {}
            
            for k in stats['res_stats']:
                self.res_stats[k] = stats['res_stats'][k]
            
            
        
        if 'datasets' in stats:
            self.datasets = stats['datasets']
        if 'resources' in stats:
            self.resources = stats['resources']  
    
      
class Resource(Model):
    
    @classmethod
    def iter(cls, iterable):
        for i in iterable:
            r = Resource.fromResult(dict(i))
            yield r
        return
    
    @classmethod
    def newInstance(cls, url=None, snapshot=None):
        props = {}
        try:
            url = urlnorm.norm(url.strip())
            #url = urllib.quote(url_norm, safe="%/:=&?~#+!$,;'@()*[]")
            # props=util.head(url)
        except Exception as e:
            log.debug('Init Resource', exctype=type(e), excmsg=e.message, url=url, snapshot=snapshot, exc_info=True)
            props['status'] = util.getExceptionCode(e)
            props['exception'] = str(type(e)) + ":" + str(e.message)

        r = cls(url=url, snapshot=snapshot, **props)
        return r

    def __init__(self, url=None, snapshot=None, **kwargs):
        super(Resource,self).__init__(**{'snapshot':snapshot,'url':url})
        self.snapshot = snapshot
        self.url = url
        self.timestamp = datetime.now()
        self.status = -1
        self.origin = None
        self.exception = None
        self.header = None
        self.mime = None
        self.size = None
        for key, value in kwargs.items():
            setattr(self, key, value)

    def updateOrigin(self, pid=None, did=None):
        if not self.origin:
            self.origin = {}
        if pid not in self.origin:
            self.origin[pid] = []
        if did not in self.origin[pid]: 
            self.origin[pid].append(did)

    def updateStats(self, stats):
        for key, value in stats.items():
            setattr(self, key, value)

    @classmethod
    def fromResult(cls, result):
        url = result['url']
        snapshot = result['snapshot']
        del result['url']
        del result['snapshot']
        
        for i in ['header', 'origin']:
            if isinstance(result[i], unicode):
                result[i] = json.loads(result[i])

        return cls(url=url,
                   snapshot=snapshot, **result)