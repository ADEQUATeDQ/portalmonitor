from collections import defaultdict, OrderedDict
import datetime


from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient
from odpw.utils.head_stats import headStats
__author__ = 'jumbrich'

from odpw.db.models import  Resource

import odpw.utils.util as util
from odpw.utils.util import getSnapshot, ErrorHandler as eh, progressIndicator

from odpw.utils.timer import Timer
from  collections import defaultdict
import structlog
log = structlog.get_logger()
from urlparse import urlparse
from time import sleep
from functools import partial
import urlnorm
from tornado import ioloop, httpclient

class DomainQueue(object):

    def __init__(self, dbm, sn, status, wait, batch):
        self.dbm=dbm
        self.sn=sn
        self.status=status
        self.queue={}
        self.wait=wait

        self.batch=batch

        self.fill()

    def fill(self):
        r= getResources(self.dbm, self.sn, status=self.status, batch=self.batch)
        for R in r:
            self.add(R)

    def add(self, R):
        url= R.url
        try:
            o = urlparse(R.url)
            loc=o.netloc
            d= self.queue.setdefault(loc,{'last':datetime.datetime.now(), 'urls':[]})
            d['urls'].append(R)
        except Exception as e:
            pass

    def __iter__(self):
        return self

    def next(self):
        while True:
            if self.size()==0 or self.size()/self.batch<1:
                print "filling Queue"
                self.fill()
            if self.size()==0:
                print "Queue is again empty, stop"
                #we filled it and still empty, we are done
                return None

            if self.hasNext():
                #print "we have a valid domain"
                d= OrderedDict(sorted(self.queue.iteritems(), key=lambda x: x[1]['last']))
                dd= d.items()[0]
                dd[1]['last']=datetime.datetime.now()
                R= dd[1]['urls'].pop()
                if len(dd[1]['urls'])==0:
                    print "remove domain from queue"
                    del self.queue[dd[0]]

                return R
            else:
                print "sleep"
                sleep(self.wait/2.0)

    def __str__(self):
        l=""+str(self.size())+" ["
        for k,v in self.queue.items():
            l +='('+k+","+v['last'].isoformat()+','+str(len(v['urls']))+')'
        l += ']'
        return l

    def __repr__(self):
        return self.__str__()

    def size(self):
        c=0
        for k,v in self.queue.items():
            c+=len(v['urls'])
        return c

    def hasNext(self):
        now= datetime.datetime.now()
        last=now
        for k,v in self.queue.items():
            if last > v['last']:
                last=v['last']
        hn= now - datetime.timedelta(seconds=self.wait) >last
        #print now-last
        return hn






#
# def head (dbm, sn, seen, resource):
#     try:
#         dbm.engine.dispose()
#         props={}
#         props['mime']=None
#         props['size']=None
#         props['redirects']=None
#         props['status']=None
#         props['header']=None
#         with Timer(key="headLookupProcessing") as t:
#             try:
#                 props=util.head(resource.url)
#             except Exception as e:
#                 eh.handleError(log, "HeadLookupException", exception=e, url=resource.url, snapshot=sn,exc_info=True)
#                 props['status']=util.getExceptionCode(e)
#                 props['exception']=util.getExceptionString(e)
#
#             resource.updateStats(props)
#             dbm.updateResource(resource)
#
#             for pid in resource.origin:
#                 if seen:
#                     d= seen[pid]
#                     if d['processed']==0:
#                         ## get the pmd for this job
#                         pmd = dbm.getPortalMetaData(portalID=pid, snapshot=sn)
#                         if not pmd:
#                             pmd = PortalMetaData(portalID=pid, snapshot=sn)
#                             dbm.insertPortalMetaData(pmd)
#                         pmd.headstart()
#                         dbm.updatePortalMetaData(pmd)
#                         d['start'] = time.time()
#
#                     d['processed']+=1
#                     if d['processed'] == d['resources']:
#                         d['end'] = time.time()
#                         pmd = dbm.getPortalMetaData(portalID=pid, snapshot=sn)
#                         if not pmd:
#                             print "AUTSCH, no pmd for ", pid
#                         pmd.headend()
#                         dbm.updatePortalMetaData(pmd)
#                     elif d['processed'] >d['resources']:
#                         print ""
#
#                     seen[pid]=d
#     except Exception as e:
#         eh.handleError(log, "HeadFunctionException", exception=e, url=resource.url, snapshot=sn,exc_info=True)

def getResources(dbm, snapshot, status=-1, batch=1000):
    resources =[]
    print "GetResources , status",status
    for res in dbm.getResourceWithoutHead(snapshot=snapshot, status=status, limit=batch):
        url='NORMALIZATION NOT POSSIBLE'
        try:

            url=urlnorm.norm(res['url'])
            R = Resource.fromResult(dict(res))
            resources.append(R)    
        except Exception as e:
            log.debug('DropHeadLookup', exctype=type(e), excmsg=e.message, url=url, snapshot=snapshot)

    return resources
    # import random
    # random.shuffle(resources)
    #
    # domaindict=defaultdict(int)
    # for R in resources:
    #
    #     loc="mis"
    #     try:
    #         o = urlparse(R.url)
    #         loc=o.netloc
    #     except Exception as e:
    #         pass
    #     domaindict[loc]+=1
    # print domaindict



def help():
    return "perform head lookups"
def name():
    return 'Head'

def setupCLI(pa):
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=1)
    pa.add_argument('--status', dest='status' , help="status ", type=int, default=-1)



done=[]
def handle_request(dbm, sn,  resource, response):

    props={}
    props['mime']=None
    props['size']=None
    props['redirects']=None
    props['status']=None
    props['header']=None
    props['exception']=None
    props['timestamp'] = datetime.datetime.now()

    error=  response.error
    if error:
        props['status']=response.code if response.code else util.getExceptionCode(error)
        props['exception']=util.getExceptionString(error)
    else:
        header_dict = dict((k.lower(), v) for k, v in dict(response.headers).iteritems())

        if 'content-type' in header_dict:
            props['mime']= util.extractMimeType(header_dict['content-type'])
        else:
            props['mime']='missing'

        props['status']=response.code
        props['header']=header_dict

        if response.code == 200:
            if 'content-length' in header_dict:
                props['size']=header_dict['content-length']
            else:
                props['size']=0

    try:
        resource.updateStats(props)

        #global done
        #if len(done)<100:
        #    done.append(resource)
        #else:
        dbm.updateResource(resource)
        print resource.url, resource.status
    except Exception as e:
        eh.handleError(log, "HeadFunctionException", exception=e, url=resource.url, snapshot=sn,exc_info=True)
        print e


#def load(http_client, dbm, sn, status):



def cli(args,dbm):

    dbm.engine.dispose()
    sn = getSnapshot(args)
    if not sn:
        return


    from urlparse import urlparse
    from threading import Thread
    import httplib, sys
    from Queue import Queue

    concurrent = 200

    def doWork():
        while True:
            url = q.get()

            res = getStatus(url.url)
            handle_request(dbm, sn, url, res)

            q.task_done()

    def getStatus(ourl):
        try:
            url = urlparse(ourl)
            conn = httplib.HTTPConnection(url.netloc)
            conn.request("HEAD", url.path)
            res = conn.getresponse()
            return res
        except:
            return "error", ourl

    def doSomethingWithResult(status, url):
        print status, url

    q = DomainQueue(dbm, sn, args.status, 5,10000)
    for i in range(concurrent):
        t = Thread(target=doWork)
        t.daemon = True
        t.start()




    #ioloop.IOLoop.instance().add_callback(load(http_client, dbm, sn, args.status))



    # while len(resources) >0:
    #     for url in open('urls.txt'):
    #         http_client.fetch(url.strip(), handle_request, method='HEAD',connect_timeout=30,
    #                                        request_timeout=30)
    #
    #
    #
    #
    #
    #     for pmd in PortalMetaData.iter(dbm.getPortalMetaDatas(snapshot=sn)):
    #         seen[pmd.portal_id]= {'resources':pmd.resources, 'processed':0}
    #
    #     log.info("Starting head lookups", count=len(resources), cores=args.processors)
    #
    #
    #
    #     results = pool.imap_unordered(head_star, resources)
    #     pool.close()
    #
    #     pool.join()
    #     for portalID,v in seen.items():
    #         if v['processed']>0:
    #             headStats(dbm, sn, portalID)
    #
    #     resources= getResources(dbm, sn, status=args.status)







