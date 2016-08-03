# -*- coding: utf-8 -*-
import datetime
from collections import defaultdict

from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from time import sleep

from scrapy.exceptions import DontCloseSpider

from new.utils.error_handling import ErrorHandler
from odpw.new.utils.helper_functions import extractMimeType

__author__ = 'jumbrich'
from odpw.new.core.api import DBClient
from odpw.new.utils.utils_snapshot import getCurrentSnapshot
from scrapy.http.request import Request
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import CrawlSpider
from scrapy.utils.project import get_project_settings

import structlog
log = structlog.get_logger()


import scrapy

class HeadLookups( CrawlSpider ):
    name = "headspider"

    def __init__(self,  category=None, *args, **kwargs):
        super(HeadLookups, self).__init__(*args, **kwargs)
        self.snapshot=kwargs['snapshot']
        self.db=kwargs['db']
        self.batch=kwargs['batch']
        dispatcher.connect(self.spider_idle, signals.spider_idle)
        self.count=0
        #print self.db, self.snapshot

    custom_settings = {
        "BATCH_INSERT":100,
        #http://doc.scrapy.org/en/latest/topics/settings.html#std:setting-SPIDER_MIDDLEWARES
        'ROBOTSTXT_ENABLED':True,
        'ROBOTSTXT_OBEY':True,
        'BOT_NAME':'',

        'USER_AGENT':'OpenDataPortalWatch (+http://data.wu.ac.at/portalwatch/about)',

        'CONCURRENT_REQUESTS':70,
        'DNSCACHE_ENABLED':True,
        'DOWNLOAD_DELAY': 0.25,
        'AUTOTHROTTLE_ENABLED':True,
        #'AUTOTHROTTLE_START_DELAY'
        #'AUTOTHROTTLE_MAX_DELAY'
        'AUTOTHROTTLE_DEBUG':True,
        #'CONCURRENT_REQUESTS_PER_DOMAIN'
        #'CONCURRENT_REQUESTS_PER_IP'
        #'DOWNLOAD_DELAY'

        'DOWNLOADER_MIDDLEWARES_BASE':{
            'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware': 100,
            #'scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware': 300,
            'scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware': 350,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 500,
            'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': 550,
            #'scrapy.downloadermiddlewares.ajaxcrawl.AjaxCrawlMiddleware': 560,
            #'scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware': 580,
            'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 590,
            'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': 600,
            #'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': 700,
            #'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 750,
            'scrapy.downloadermiddlewares.chunked.ChunkedTransferMiddleware': 830,
            'scrapy.downloadermiddlewares.stats.DownloaderStats': 850,
            #'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware': 900,
            'odpw.new.core.head_middlewares.ErrorHandling': 852,
        },
        'DOWNLOAD_HANDLERS': {
            'file': None, 's3': None, 'ftp': None
        },
        'ITEM_PIPELINES': {
            'odpw.new.core.head_middlewares.PostgresInsert': 300
        }
    }

    def scheduleURLs(self):
        log.info("Scheduling")
        q=self.db.getUnfetchedResources(self.snapshot, batch=self.batch)
        uris= [ uri[0] for uri in q ]

        stats=defaultdict(int)

        for u in uris:
            try:
                from urlparse import urlparse
                parsed_uri = urlparse( 'http://stackoverflow.com/questions/1234567/blah-blah-blah-blah' )
                domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
                stats[domain]+=1
            except:
                pass
            r= Request(u,method='HEAD',
                      # callback=self.success,
                      # errback=self.error,
                      dont_filter=True,
                      meta={'handle_httpstatus_all':True})
            self.crawler.engine.schedule(r,self)

        log.info("Scheduled", uris=len(uris), stats=stats)

    def spider_idle(self, spider):
        try:
            self.scheduleURLs()
            raise DontCloseSpider
        except Exception as e:
            ErrorHandler.handleError('spider_idle',exception=e)

        #new_url = ... #pop url from  database
        #if new_url:
        #    self.crawler.engine.schedule(
        #        Request(new_url, callback=self.parse), spider)
        #    raise DontCloseSpider


    def start_requests(self):
        print 'here'
        q=self.db.getUnfetchedResources(self.snapshot, batch=self.batch)
        uris= [ uri[0] for uri in q ]
        stats=defaultdict(int)
        for u in uris:
            try:
                from urlparse import urlparse
                parsed_uri = urlparse( 'http://stackoverflow.com/questions/1234567/blah-blah-blah-blah' )
                domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
                stats[domain]+=1
            except:
                pass
            yield Request(u,method='HEAD',
                          # callback=self.success,
                          # errback=self.error,
                          dont_filter=True,
                          meta={'handle_httpstatus_all':True})
        print 'here'
        log.info("InitScheduled", uris=len(uris), stats=stats)
    def parse(self,response):
        r={
            'snapshot':self.snapshot
            ,'uri':response.url
            ,'timestamp':datetime.datetime.now()
            ,'status':response.status
            ,'exc':response.meta['exc'] if 'exc' in response.meta else None
            ,'header':response
            ,'mime':None
            ,'size':None
        }

        try:
            header_dict = dict((k.lower(), v) for k, v in dict(response.headers).iteritems())

            if 'content-type' in header_dict and len(header_dict['content-type'])>0:
                r['mime']= extractMimeType(header_dict['content-type'][0])
            else:
                r['mime']='missing'

            r['header']=header_dict

            if response.status == 200:
                if 'content-length' in header_dict and len(header_dict['content-length'])>0:
                    r['size']=header_dict['content-length'][0]
                else:
                    r['size']=0
        except Exception as e:
            ErrorHandler.handleError('parse',exception=e)

        self.count+=1
        if self.count  > (self.batch/2):
            try:
                self.scheduleURLs()
                self.count=0
            except Exception as e:
                ErrorHandler.handleError('spider_idle',exception=e)
        return r








def help():
    return "perform head lookups"
def name():
    return 'Head1'

def setupCLI(pa):
    pa.add_argument("-t","--threads",  help='Number of threads',  dest='threads', default=4,type=int)
    pa.add_argument("-b","--batch",  help='Batch size',  dest='batch', default=10000,type=int)
    pa.add_argument("-d","--delay",  help='Default domain delay (in sec)',  dest='delay', default=5,type=int)

def cli(args, dbm):
    sn = getCurrentSnapshot()
    db=DBClient(dbm)


    settings=get_project_settings()
    crawler = CrawlerProcess(settings)

    crawler.crawl(HeadLookups,snapshot=sn, db=db, batch=args.batch)

    crawler.start()