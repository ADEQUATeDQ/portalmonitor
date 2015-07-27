
__author__ = 'jumbrich'

from sqlalchemy.engine.result import RowProxy

import urlnorm
import odpw.util as util
from odpw.util import ErrorHandler as eh

from datetime import datetime

import logging
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()
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
        del result['url']
        del result['apiurl']

        return cls(url=url,
                   apiurl=apiurl, **result)

    @classmethod
    def newInstance(cls, url, apiurl, software):
        props = {
            'datasets':-1,
            'status':-1,
            'exception':None,
            'software':software,
            'resources':-1,
            'changefeed':False
        }

        # TODO detect changefeed
        p = cls(url,
                   apiurl,
                   country=util.getCountry(url),
                   **props


        )
        log.info("new portal instance", pid=p.id, apiurl=p.apiurl)
        return p


    def __init__(self, url, apiurl, **kwargs):
        super(Portal,self).__init__(**{'url':url,'apiurl':apiurl})
        
        self.id = util.computeID(url)
        self.url = url
        self.latest_snapshot = None
        self.apiurl = apiurl
        self.software = None
        self.datasets = -1
        self.status = -1
        self.exception = None
        self.resources = -1
        self.changefeed = False
        
        for key, value in kwargs.items():
            setattr(self, key, value)

class Dataset(Model):
    
    @classmethod
    def iter(cls, iterable):
        for i in iterable:
            r = Dataset.fromResult(dict(i))
            yield r
        return
    
    def __init__(self, snapshot, portal, dataset, data=None, **kwargs):
        super(Dataset,self).__init__(**{'snapshot':snapshot,'portal':portal,'dataset':dataset})
        
        self.snapshot = snapshot
        self.portal = portal
        self.dataset = dataset
        
        self.data = data
        self.status = None
        self.exception = None
        self.md5 = None
        self.change = None
        self.fetch_time = None
        self.qa = None
        self.qa_time = None
        
        
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    @classmethod
    def fromResult(cls, result):
        if isinstance(result, RowProxy):
            result = dict(result)
        
        
        snapshot = result['snapshot']
        portal = result['portal']
        dataset = result['dataset']
        
        del result['portal']
        del result['dataset']
        del result['snapshot']

        for i in ['data', 'qa']:
            if isinstance(result[i], unicode):
                result[i] = json.loads(result[i])

        return cls(dataset=dataset, portal=portal,
                   snapshot=snapshot, **result)      
        
class PortalMetaData(Model):

    @classmethod
    def iter(cls, iterable):
        for i in iterable:
            r = PortalMetaData.fromResult(dict(i))
            yield r
        return

    @classmethod
    def fromResult(cls, result):
        portal = result['portal']
        snapshot = result['snapshot']
        del result['portal']
        del result['snapshot']


        for i in ['res_stats', 'qa_stats', 'general_stats', 'fetch_stats']:
            if isinstance(result[i], unicode):
                result[i] = json.loads(result[i])

        return cls(portal=portal,
                   snapshot=snapshot, **result)

    def __init__(self, portal=None, snapshot=None, **kwargs):
        super(PortalMetaData,self).__init__(**{'snapshot':snapshot,'portal':portal})
        self.snapshot = snapshot
        self.portal = portal
        self.res_stats = None
        self.qa_stats = None
        self.general_stats = None
        self.exception = None
        self.resources = -1
        self.datasets = -1
        self.fetch_stats = None
        
        for key, value in kwargs.items():
            setattr(self, key, value)

    def fetchstart(self):
        if self.fetch_stats:
            self.fetch_stats['fetch_start'] = datetime.now().isoformat()
        else:
            self.fetch_stats = {'fetch_start':datetime.now().isoformat()}
    
    def fetchend(self):
        if self.fetch_stats:
            self.fetch_stats['fetch_end'] = datetime.now().isoformat()
        else:
            self.fetch_stats = {'fetch_end':datetime.now().isoformat()}
        
        
   
            
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
    def newInstance(cls, url=None, snapshot=None):
        props = {}
        try:
            url = urlnorm.norm(url)
            # props=util.head(url)
        except Exception as e:
            log.error('Init Resource', exctype=type(e), excmsg=e.message, url=url, snapshot=snapshot, exc_info=True)
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
        self.redirects = None
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
        
        for i in ['header', 'origin', 'redirects']:
            if isinstance(result[i], unicode):
                result[i] = json.loads(result[i])

        return cls(url=url,
                   snapshot=snapshot, **result)

if __name__ == '__main__':
    logging.basicConfig()
    # http://services1.arcgis.com/0g8o874l5un2eDgz/arcgis/rest/services/PublicSlipways/FeatureServer/0/query?outFields=*&where=1%3D1
    # r = Resource.newInstance(url="http://polleres.net/foaf.rdf", snapshot='2015-10')
    # print r.__dict__
    
    p1 = Portal("http",'1')
    p2 = Portal("http",'1')
    p3 = Portal("http",'2')
    
    print p1.__hash__()
    print p2.__hash__()
    print p3.__hash__()
    print p1 == p3



