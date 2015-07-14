import ckanapi
from urlparse import urlparse
__author__ = 'jumbrich'



import urlnorm
import odpw.util as util
from odpw.util import ErrorHandler as eh

import requests
from datetime import datetime
import logging
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()
import json
import urlparse

class Portal:

    @classmethod
    def fromResult(cls, result):
        
        url=result['url']
        apiurl=result['apiurl']
        del result['url']
        del result['apiurl']

        return cls(url=url,
                   apiurl=apiurl,**result)

    @classmethod
    def newInstance(cls,url=None, apiurl=None, software='CKAN'):
        props={
            'datasets':-1,
            'status':-1,
            'exception':None,
            'software':software,
            'resources':-1,
            'changefeed':False
        }
        try:
            
            
            package_list, status = util.getPackageList(apiurl)
            
            props['status']=status
            props['datasets']=len(package_list)
            log.info('Received packages', apiurl=apiurl, status=status,count=props['datasets'])
        except Exception as e:
            eh.handleError(log, 'fetching dataset information', exception=e,apiurl=apiurl,exc_info=True)
            props['status']=util.getExceptionCode(e)
            props['exception']=str(type(e))+":"+str(e.message)

        #TODO detect changefeed
        p=cls(url=url,
                   apiurl=apiurl,
                   country=util.getCountry(url),
                   **props


        )
        log.info("new portal instance",pid=p.id, apiurl=p.apiurl)
        return p


    def __init__(self, url=None, apiurl=None, **kwargs):
        self.id=util.computeID(url)
        self.url=url
        self.latest_snapshot=None
        self.apiurl=apiurl
        self.software=None
        self.datasets=-1
        self.status=-1
        self.exception=None
        self.resources=-1
        self.changefeed=False
        
        for key, value in kwargs.items():
            setattr(self, key, value)



class Dataset:
    def __init__(self, snapshot=None, portal=None, dataset=None,**kwargs):
        self.snapshot=snapshot
        self.portal=portal
        self.dataset=dataset
        
        self.data=None
        self.status=None
        self.exception=None
        self.md5=None
        self.change=None
        self.fetch_time=None
        self.qa=None
        self.qa_time=None
        
        
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    @classmethod
    def fromResult(cls, result):
        snapshot=result['snapshot']
        portal=result['portal']
        dataset=result['dataset']
        
        del result['portal']
        del result['dataset']
        del result['snapshot']

        for i in ['data','qa']:
            if isinstance(result[i], unicode ):
                result[i] = json.loads(result[i])

        return cls(portal=portal,
                   snapshot=snapshot,**result)      
    

class PortalMetaData:

    @classmethod
    def fromResult(cls, result):
        portal=result['portal']
        snapshot=result['snapshot']
        del result['portal']
        del result['snapshot']


        for i in ['res_stats','qa_stats','general_stats','fetch_stats']:
            if isinstance(result[i], unicode ):
                result[i] = json.loads(result[i])

        return cls(portal=portal,
                   snapshot=snapshot,**result)

    def __init__(self, portal=None, snapshot=None, **kwargs):
        self.snapshot=snapshot
        self.portal=portal
        self.res_stats=None
        self.qa_stats=None
        self.general_stats=None
        self.exception=None
        self.resources=-1
        self.datasets=-1
        self.fetch_stats=None
        
        for key, value in kwargs.items():
            setattr(self, key, value)

    def fetchstart(self):
        if self.fetch_stats:
            self.fetch_stats['fetch_start']=datetime.now().isoformat()
        else:
            self.fetch_stats={'fetch_start':datetime.now().isoformat()}
        
        
    def updateStats(self, stats):
        if 'fetch_stats' in stats:
            if not self.fetch_stats:
                self.fetch_stats={}
            self.fetch_stats['fetch_end']=datetime.now().isoformat()
            self.fetch_stats['fullfetch']=stats['fetch_stats']['fullfetch']
            self.fetch_stats['respCodes']=stats['fetch_stats']['respCodes']
            self.fetch_stats['datasets']=stats['datasets']
            self.fetch_stats['portal_status']=stats['status']

        if 'qa_stats' in stats:
            if not self.qa_stats:
                self.qa_stats={}
            for key in stats['qa_stats'].keys():
                self.qa_stats[key]=stats['qa_stats'][key]
        

        if 'general_stats' in stats:
            if not self.general_stats:
                self.general_stats={}
            for key in stats['general_stats'].keys():
                self.general_stats[key]=stats['general_stats'][key]
        
        
        if 'res_stats' in stats:
            if not self.res_stats:
                self.res_stats={}
            self.res_stats['respCodes']=stats['res_stats']['respCodes']
            self.res_stats['total']=stats['res_stats']['total']
            self.res_stats['unique']=len(stats['res_stats']['resList'])
        
        if 'datasets' in stats:
            self.datasets=stats['datasets']
        if 'resources' in stats:
            self.resources=stats['resources']  
    
      
class Resource:
    @classmethod
    def newInstance(cls, url=None, snapshot=None):
        props={}
        try:
            url=urlnorm.norm(url)
            #props=util.head(url)
        except Exception as e:
            log.error('Init Resource', exctype=type(e), excmsg=e.message, url=url, snapshot=snapshot,exc_info=True)
            props['status']=util.getExceptionCode(e)
            props['exception']=str(type(e))+":"+str(e.message)

        r = cls(url=url, snapshot=snapshot,**props)
        log.info("new portal instance",url=r.url, snapshot=r.snapshot)
        return r

    def __init__(self, url=None, snapshot=None, **kwargs):
        self.snapshot=snapshot
        self.url=url
        self.timestamp=datetime.now()
        self.status=-1
        self.origin=None
        self.exception=None
        self.header=None
        self.mime=None
        self.redirects=None
        self.size=None
        for key, value in kwargs.items():
            setattr(self, key, value)

    def updateOrigin(self,pid=None, did=None):
        if not self.origin:
            self.origin={}
        if pid not in self.origin:
            self.origin[pid]=[]
        if did not in self.origin[pid]: 
            self.origin[pid].append(did)

    def updateStats(self, stats):
        for key, value in stats.items():
            setattr(self, key, value)

    @classmethod
    def fromResult(cls, result):
        url=result['url']
        snapshot=result['snapshot']
        del result['url']
        del result['snapshot']
        
        for i in ['header','origin','redirects']:
            if isinstance(result[i], unicode ):
                result[i] = json.loads(result[i])

        return cls(url=url,
                   snapshot=snapshot,**result)

if __name__ == '__main__':
    logging.basicConfig()
    #http://services1.arcgis.com/0g8o874l5un2eDgz/arcgis/rest/services/PublicSlipways/FeatureServer/0/query?outFields=*&where=1%3D1
    #r = Resource.newInstance(url="http://polleres.net/foaf.rdf", snapshot='2015-10')
    #print r.__dict__
    
    
    




