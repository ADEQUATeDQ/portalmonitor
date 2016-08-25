
import structlog
import twisted

log = structlog.get_logger()
import scrapy
from scrapy.http import Response

from odpw.new.utils.error_handling import getExceptionString, ErrorHandler
from odpw.new.core.model import ResourceInfo

error_classes=[
    (KeyError,601,None)
    ,(scrapy.exceptions.IgnoreRequest,602,'Robots.txt')
    ,(scrapy.exceptions.NotSupported,603)
    ,(twisted.internet.error.ConnectError,604)
    ,(twisted.internet.error.ConnectionRefusedError,605)
    ,(twisted.internet.error.DNSLookupError,606)
    ,(twisted.internet.error.TCPTimedOutError,607)
    ,(twisted.internet.error.TimeoutError,608)
    ,(twisted.web._newclient.ResponseFailed,609)
    ,(twisted.web._newclient.ResponseNeverReceived,610)
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

class PostgresInsert(object):
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
            log.info("HEAD RESPONSE", uri=RI.uri, status=RI.status, exc=RI.exc)


        except Exception as e:
            ErrorHandler.handleError(log, 'process_item',exception=e)

    def bulkInsert(self,spider):
        try:
            log.info("BulkInsert", size=len(self.items))
            spider.db.bulkadd(self.items)
            self.items=[]
        except Exception as e:
            ErrorHandler.handleError(log, 'bulkInsert',exception=e)
