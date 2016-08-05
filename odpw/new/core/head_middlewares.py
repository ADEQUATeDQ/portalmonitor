
import structlog
import twisted

log = structlog.get_logger()
import scrapy
from scrapy.http import Response

from odpw.new.utils.error_handling import getExceptionString, ErrorHandler
from odpw.new.core.model import ResourceInfo

class ErrorHandling(object):


    def process_response(self, request, response, spider):
        return response

    def process_exception(self, request, exception, spider):
        ErrorHandler.handleError(log, 'process_exception',exception=exception, uri=request.url)
        status=600
        if isinstance(exception, KeyError):
            status=601
        elif isinstance(exception, scrapy.exceptions.IgnoreRequest):
            status=602
            request.meta['exc']='Robots.txt'
        elif isinstance(exception,scrapy.exceptions.NotSupported):
            status=603
            request.meta['exc']='Not_supported protocol'
        elif isinstance(exception,twisted.internet.defer.CancelledError):
            return Response(url=request.url, status=200,  request=request, headers=request.headers)
        elif isinstance(exception,twisted.internet.error.DNSLookupError):
            status=604
            request.meta['exc']='DNSLookup'
        elif isinstance(exception,    twisted.internet.error.ConnectError):
            status=605
            request.meta['exc']='Connection lost'
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
        log.info("Opeing Middleware", spider=spider)

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
