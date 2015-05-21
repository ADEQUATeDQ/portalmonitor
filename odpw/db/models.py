__author__ = 'jumbrich'



import urlnorm
import odpw.util as util
from odpw import ckanclient
import requests
from datetime import datetime
import logging
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()

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
            resp = ckanclient.package_get(apiurl)
            props['status']=resp.status_code

            if resp.status_code != requests.codes.ok:
                log.error("No package list received", apiurl=apiurl, status=resp.status_code)
            else:
                package_list = resp.json()
                props['datasets']=len(package_list)
                log.info('Received packages', apiurl=apiurl, status=resp.status_code,count=props['datasets'])
        except Exception as e:
            print 'here'
            #log.warning("fetching dataset information", apiurl=apiurl, exctype=type(e), excmsg=e.message)
            log.error('fetching dataset information', apiurl=apiurl,exctype=type(e), excmsg=e.message,exc_info=True)
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
        for key, value in kwargs.items():
            setattr(self, key, value)


class Dataset:
    def __init__(self, snapshot=None, portal=None, dataset=None,**kwargs):
        self.snapshot=snapshot
        self.portal=portal
        self.dataset=dataset
        for key, value in kwargs.items():
            setattr(self, key, value)

class PortalMetaData:

    @classmethod
    def fromResult(cls, result):
        portal=result['portal']
        snapshot=result['snapshot']
        del result['portal']
        del result['snapshot']

        return cls(portal=portal,
                   snapshot=snapshot,**result)

    def __init__(self, portal=None, snapshot=None, **kwargs):
        self.snapshot=snapshot
        self.portal=portal
        self.res_stats={}
        self.qa_stats={}
        self.general_stats={}
        self.exception=None

        self.fetch_stats={'fetch_start':datetime.now()}
        for key, value in kwargs.items():
            setattr(self, key, value)

    def updateFetchStats(self, stats):
        if 'fetch_stats' in stats:
            self.fetch_stats['fetch_end']=datetime.now()
            self.fetch_stats['fullfetch']=stats['fetch_stats']['fullfetch']
            self.fetch_stats['respCodes']=stats['fetch_stats']['respCodes']
            self.fetch_stats['datasets']=stats['datasets']

        if 'general_stats' in stats:
            self.general_stats['keys']=stats['general_stats']['keys']
        if 'res_stats' in stats:
            self.res_stats['respCodes']=stats['res_stats']['respCodes']
            self.res_stats['total']=stats['res_stats']['total']
            self.res_stats['unique']=len(stats['res_stats']['resList'])

    @classmethod
    def fromResult(cls, result):
        portal=result['portal']
        snapshot=result['snapshot']
        del result['portal']
        del result['snapshot']

        return cls(portal=portal,
                   snapshot=snapshot,**result)

class Resource:
    @classmethod
    def newInstance(cls, url=None, snapshot=None):
        props={}
        try:
            url=urlnorm.norm(url)
            props=util.head(url)

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
            self.origin[pid]=[did]
        else:
            self.origin[pid].append(did)









    @classmethod
    def fromResult(cls, result):
        url=result['url']
        snapshot=result['snapshot']
        del result['url']
        del result['snapshot']

        return cls(url=url,
                   snapshot=snapshot,**result)



if __name__ == '__main__':
    logging.basicConfig()
    r = Resource.newInstance(url="http://services1.arcgis.com/0g8o874l5un2eDgz/arcgis/rest/services/PublicSlipways/FeatureServer/0/query?outFields=*&where=1%3D1", snapshot='2015-10')
    print r.__dict__




