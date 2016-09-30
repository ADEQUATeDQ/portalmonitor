import datetime
import gzip
import hashlib
import os
import time
import urllib
from urlparse import urlparse

import structlog
import sys
import twisted

log = structlog.get_logger()
import scrapy
from scrapy.http import Response

from odpw.utils.error_handling import getExceptionString, ErrorHandler
from odpw.core.model import ResourceInfo, ResourceCrawlLog

error_classes=[
    (KeyError,601,None)
    ,(scrapy.exceptions.IgnoreRequest,602,'Robots.txt')
    ,(scrapy.exceptions.NotSupported,603)
    ,(twisted.internet.error.ConnectError,604)
    ,(twisted.internet.error.ConnectionRefusedError,605)
    ,(twisted.internet.error.DNSLookupError,606)
    ,(twisted.internet.error.TCPTimedOutError,607)
    ,(twisted.internet.error.TimeoutError,608)
    ]

class ErrorHandling(object):
    def process_response(self, request, response, spider):
        return response

    def process_exception(self, request, exception, spider):
        ErrorHandler.handleError(log, 'process_exception',exception=exception, uri=request.url)
        status=600
        if isinstance(exception,twisted.internet.defer.CancelledError):
            return Response(url=request.url, status=200,  request=request, headers=request.headers)
        for t in error_classes:
            if isinstance(exception, t[0]):
                status=t[1]
                request.meta['exc']=str(type(exception)) if len(t)==2 else t[2]
                return Response(url=request.url, status=status, request=request)
        else:
            request.meta['exc']=str(type(exception))
            return Response(url=request.url, status=status, request=request)

class ResourceInfoInserter(object):
    def __init__(self, batch):
        self.items=[]
        self.batch=batch
        self.inserted=0

    @classmethod
    def from_crawler(cls, crawler):
        batch=100
        if crawler.settings.getbool('BATCH_INSERT'):
            batch = crawler.settings.getint('BATCH_INSERT', 100)
        ext= cls(batch)

        # return the extension object
        return ext

    def open_spider(self, spider):
        log.info("Opening Middleware", spider=spider)

    def close_spider(self, spider):
        if len(self.items) >= 0:
            self.bulkInsert(spider)
        log.info("Closing Middleware", spider=spider)


    def process_item(self, item, spider):
        try:
            r={ 'snapshot':item['snapshot']
                ,'uri':item['uri']
                ,'timestamp':item['timestamp']
                ,'status':item['status']
                ,'exc':item['exc']
                ,'header':item['header']
                ,'mime':item['mime']
                ,'size':item['size']
            }
            RI=ResourceInfo(**r)

            spider.db.add(RI)
            self.inserted+=1
            log.info("HEAD RESPONSE", uri=RI.uri, status=RI.status, exc=RI.exc,inserted=self.inserted)

        except Exception as e:
            ErrorHandler.handleError(log, 'process_item',exception=e)

    def bulkInsert(self,spider):
        try:
            log.info("BulkInsert", size=len(self.items))
            spider.db.bulkadd(self.items)
            self.items=[]
        except Exception as e:
            ErrorHandler.handleError(log, 'bulkInsert',exception=e)



class FileDownloader(object):

    def __init__(self, stats):
        super(FileDownloader, self).__init__()
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

    def process_response(self, request, response, spider):
        status=response.status
        request.meta['contentchanged']=-1

        if 'robots.txt' in response.url:
            self.stats.inc_value('robots')
            self.stats.inc_value('robot_status/'+str(response.status))

        if status==200 and 'robots.txt' not in response.url:

            if hasattr(response,'body_as_unicode'):
                content=response.body_as_unicode()
            else:
                content=response.body
            #build new response
            request.meta['error']=None

            if 'domain' not in request.meta:
                domain=''
                try:
                    parsed_uri = urlparse( response.url )
                    domain = '{uri.netloc}'.format(uri=parsed_uri)
                except:
                    domain='error'
                request.meta['domain']=domain
            #create folder and file
            domain=request.meta['domain']

            #compute digest and filesize
            digest=hashlib.md5(content).hexdigest()
            request.meta['digest']=digest
            request.meta['size']=sys.getsizeof(content)

            #check if digest exists?, if yes, get file location and file size
            loc= spider.api.getContentLocation(uri=response.url,digest=digest)
            last_digest= spider.api.getLastDigest(uri=response.url)

            request.meta['contentchanged']=0 if last_digest and last_digest[0]==digest else 1
            if loc:
                request.meta['disk']=loc[0]
            else:
                try:
                    filename=os.path.join(spider.datadir,self.getFileName(request.url,domain=domain))
                    if not os.path.exists(os.path.dirname(filename)):
                        os.makedirs(os.path.dirname(filename))

                    request.meta['disk']=filename
                    with open(filename,'wb') as fw:
                        try:
                            fw.write(content)
                        except Exception as e:
                            request.meta['error']=getExceptionString(e)
                            ErrorHandler.handleError("Writing file", exception=e)
                except Exception as e:
                    ErrorHandler.handleError(log,'file_download',exception=e, uri=request.url)
                    request.meta['error']=getExceptionString(e)
                    status=606
                    return Response(url=request.url, status=status, headers=response.headers, request=request)

        r= Response(url=response.url,status=response.status, headers=response.headers, request=request)
        return r

    def getFileName(self, uri, domain=None):

        now=datetime.datetime.now()
        datefolders=now.strftime("%Y/%m/%d/%H")

        urlencoded = urllib.quote_plus(uri)[:240]
        log.info("Encoded url "+urlencoded);

        fileLocation = os.path.join(datefolders,domain,urlencoded)
        log.info("FileLocation", uri=uri, disk=fileLocation)
        return fileLocation

current_milli_time = lambda: int(round(time.time() * 1000))
class DownloadTimer(object):

    def process_request(self, request, spider):
        request.meta['__start_time'] = current_milli_time()
        # this not block middlewares which are has greater number then this
        return None

    def process_response(self, request, response, spider):
        request.meta['__end_time'] = current_milli_time()
        #return Response(url=request.url, request=request)
        return response  # return response coz we should



class CrawlLogInserter(object):
    def __init__(self, batch):
        self.items=[]
        self.batch=batch

    @classmethod
    def from_crawler(cls, crawler):
        batch=100
        if crawler.settings.getbool('BATCH_INSERT'):
            batch = crawler.settings.getint('BATCH_INSERT', 100)
        ext= cls(batch)

        # return the extension object
        return ext

    def open_spider(self, spider):
        log.info("Opeing Middleware", spider=spider)

    def close_spider(self, spider):
        if len(self.items) >= 0:
            self.bulkInsert(spider)
        log.info("Closing Middleware", spider=spider)


    def process_item(self, item, spider):
        try:
            cl=ResourceCrawlLog(**item)

            self.items.append(cl)
            if len(self.items) >= self.batch:
                self.bulkInsert(spider)
        except Exception as e:
            ErrorHandler.handleError(log,'process_item',exception=e)

    def bulkInsert(self,spider):
        try:
            log.info("BulkInsert", size=len(self.items))
            spider.api.bulkadd(self.items)
            self.items=[]
        except Exception as e:

            ErrorHandler.handleError(log,'bulkInsert',exception=e)
