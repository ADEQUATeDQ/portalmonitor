# -*- coding: utf-8 -*-

from collections import OrderedDict
import datetime
import requests
from requests.exceptions import InvalidSchema

__author__ = 'jumbrich'

from odpw.db.models import  Resource

import odpw.utils.util as util
from odpw.utils.util import getSnapshot, ErrorHandler as eh

from odpw.utils.timer import Timer
import structlog
log = structlog.get_logger()
from time import sleep
import urlnorm
from reppy.cache import RobotsCache
from threading import  current_thread
from urlparse import urlparse
import time

#import socket
#socket.setdefaulttimeout(5)
from contextlib import closing


user_agent='OpenDataPortalWatch (+http://data.wu.ac.at/portalwatch/about)'
headers = {
    'User-Agent': user_agent,
    'From': 'contact@data.wu.ac.at'  # This is another valid field
}
getheaders={"range": "bytes=3-5",'Accept-Encoding': 'identity'}.update(headers)

import threading


def timelimit(timeout):
    def internal(function):
        def internal2(*args, **kw):
            class Calculator(threading.Thread):
                def __init__(self):
                    threading.Thread.__init__(self)
                    self.result = None
                    self.error = None

                def run(self):
                    try:
                        self.result = function(*args, **kw)
                    except Exception as e:
                        self.error = e

            c = Calculator()
            c.start()
            c.join(timeout)
            if c.isAlive():
                e=util.TimeoutError(message="Processing"+str(function),timeout=timeout)
                raise e
            if c.error:
                print "With error",c.error
                raise c.error
            return c.result
        return internal2
    return internal


class DomainQueue(object):

    def __init__(self,  wait):
        self.queue={}
        self.waits={}
        self.wait=wait
        self.seen=set([])

        self.done=False

    def get(self):
        while True:
            if self.size()==0 and self.done:
                log.info("Queue is empty!!!")
            elif self.size()==0 and not self.done:
                log.info("Queue is empty but not done!!!")

            d= OrderedDict(sorted(self.queue.iteritems(), key=lambda x: x[1]['last']))
            for dd in d.items():
                if len(dd[1]['urls'])!=0:
                    now = datetime.datetime.now()
                    if now - dd[1]['last'] > datetime.timedelta(seconds=self.waits[dd[0]]):
                        dd[1]['last']=now
                        if len(dd[1]['urls'])!=0:
                            R= dd[1]['urls'].pop()
                            self.seen.add(R.url)
                            return R
            sleep(self.wait/2.0)

    def done(self):
        self.done=True

    def addWait(self, url, wait):
        try:
            o = urlparse(url)
            loc=o.netloc
            delay = wait if wait is not None else self.wait
            if loc not in self.waits:
                log.info("DelayInfo", domain=loc, delay=delay)
                self.waits[loc]=delay
        except Exception as e:
            pass

    def put(self, R):
        try:
            o = urlparse(R.url)
            loc=o.netloc
            if loc not in self.waits:
                self.addWait(R.url,None)
            if R.url not in self.seen:
                d= self.queue.setdefault(loc,{'last':datetime.datetime.now(), 'urls':set([])})
                d['urls'].add(R)
                log.info("ADD", url=R.url)
                return True

        except Exception as e:
            eh.handleError(log,"ADD2QUEUE", exception=e, url=R.url)
            pass
        return False

    def urlsSeen(self):
        return len(self.seen)

    def info(self):
        return "Queue (size:"+str(self.size())+", domains:"+str(self.domains())+", seen:"+str(self.urlsSeen())+")"

    def __str__(self):
        l="Queue ("+str(self.size())+", "+str(self.domains())+", "+str(len(self.seen))+") ["
        for k,v in self.queue.items():
            l +='('+k.encode('utf-8')+","+v['last'].isoformat()+','+str(len(v['urls']))+','+str(self.waits[k] if k in self.waits else "nys")+')'
        l += ']'
        return l

    def __repr__(self):
        return self.__str__()

    def domains(self):
        n=0
        for d, s in self.queue.iteritems():
            if len(s['urls'])>=0:
                n+=1
        log.info("Queue status", domains= len(self.queue.keys()), nonempty=n)
        return n

    def size(self):
        c=0
        for k,v in self.queue.items():
            c+=len(v['urls'])
        return c






def help():
    return "perform head lookups"
def name():
    return 'Head'

def setupCLI(pa):
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    pa.add_argument("-c","--cores", type=int, help='Number of processors to use', dest='processors', default=1)
    pa.add_argument('--status', dest='status' , help="status ", type=int, default=-1)


statuscount={'total':{}}


from threading import Thread

class QueueFiller(Thread):
    def __init__(self, dbm,q, robots, batch, status, sn, concurrent):
        Thread.__init__(self)

        self.dbm=dbm
        self.queue=q
        self.batch=batch
        self.robots=robots
        self.status=status
        self.snapshot=sn
        self.workers=concurrent

        self.starttime=time.time()
        self.interim=time.time()
        self.lastSeen=0


    def run(self):
        self.start=time.time()

        log.info("FILLTHREAD running")
        c=0
        while True:
            domain=self.queue.domains()
            size=self.queue.size()
            fill= size==0 or size/(self.batch*1.0)<0.5 or domain<self.workers*2
            log.info("QueueInfo", queue=self.queue.info(), fill=fill, size=size, domain=domain)

            if fill:
                self.filling_queue(self.status, self.batch*2)
                self.statusInfo()

            if self.queue.size()==0:
                return

            c+=1
            sleep(10)
            if c % (6*5) == 0:
                self.statusInfo()

        log.info("FILLTHREAD stopping")
        self.queue.done()

    def statusInfo(self):

        seen= self.queue.urlsSeen()
        now=time.time()

        elapsed = (now - self.starttime)
        interim=(now-self.interim)

        s=[ "SYSTEM STATUS","\n"
            ,self.queue.info()
            ," Robots",str(len(self.robots.robots._cache.keys())),"\n"
            ," active_threads",str(threading.activeCount()),"\n"
            ," statuscount_total",str(statuscount['total']),"\n"
            ," statuscount",str(statuscount),"\n"
            ," -------------------------","\n"
            ," Running since:", util.timer(elapsed),"\n"
            ,"   processed:", str(seen),"\n"
            ,"   performance:", str(seen/(1.0*elapsed))," urls/sec","\n"
            ," since_last_update:", util.timer(interim),"\n"
            ,"   processed:", str(seen-self.lastSeen),"\n"
            ,"   performance:", str((seen-self.lastSeen)/(1.0*interim))," urls/sec","\n"
            ," -------------------------","\n"
            ," Timing stats:","\n"
        ]
        for m, st in Timer.getStats().items():
            d=[    "  ["+m+'] -', str(st['avg']),'avg ms for',m,str(st['calls']),'calls)'
                ,"\n       (min:",str(st['min']),"-",str(st['max'])," max)\n"]
            s=s+d


        s.append(" -------------------------\n")
        s.append(" Exceptions\n")
        for exc, count in eh.exceptions.iteritems():
            s.append("  ")
            s.append(exc)
            s.append(str(count))
            s.append("\n")


        print " ".join(s)

        log.info("STATUS", q=self.queue.info(), robots=len(self.robots.robots._cache.keys()), active=threading.activeCount())
        #Timer.printStats()
        #eh.printStats()

        self.interim=now
        self.lastSeen=seen




    def filling_queue(self, status, batch):
        with Timer(key='filling_queue'):
            log.info("Filling", thread=True)
            r= self.getResources(status=status, batch=batch)
            log.info("Received", result_len= len(r))
            c={True:0, False:0}
            for R in r:
                c[self.queue.put(R)]+=1
            log.info("Filled", thread=True, queue=self.queue, count=c)

    def getResources(self, status=-1, batch=1000):
        resources =[]
        for res in self.dbm.getResourceWithoutHead(snapshot=self.snapshot, status=status, limit=batch):
            url='NORMALIZATION NOT POSSIBLE'
            try:
                url=urlnorm.norm(res['url'])
                R = Resource.fromResult(dict(res))
                resources.append(R)
            except Exception as e:
                log.debug('DropHeadLookup', exctype=type(e), excmsg=e.message, url=url, snapshot=self.snapshot)
        log.info("loaded", resources=len(resources))
        return resources


class Worker(Thread):


    def __init__(self, q,dbm, sn, robots, rsession):
        Thread.__init__(self)

        self.queue=q
        self.dbm=dbm
        self.sn=sn
        self.robots=robots
        self.rsession=rsession

    def run(self):
        log.info("Started", thread=current_thread())
        while True:
            with Timer(key="doWork"):
                try:
                    with Timer(key="q.get"):
                        url = self.queue.get()
                    if url is None:
                        break
                    try:
                        #check if this is a valid URL, if we have an exception, done
                        o = urlparse(url.url)
                        if o.scheme.startswith('http'):
                            if self.checkUpdateRobots(url):
                                res = self.getStatus(url.url)
                                self.handle_request(url, res, None)
                            else:
                                log.info("Robots Denied", url=url.url)
                                props={}
                                props['mime']=None
                                props['size']=None
                                props['redirects']=None

                                props['header']=None
                                props['exception']=None
                                props['timestamp'] = datetime.datetime.now()
                                props['status']=666

                                with Timer(key="updateResourceStatus") as t:
                                    url.updateStats(props)
                                    self.dbm.updateResource(url)

                                    for k in ['total',o.netloc]:
                                        t=statuscount.setdefault(k,{})
                                        if props['status'] not in t:
                                            t[props['status']]=0
                                        t[props['status']]+=1

                        else:
                            raise InvalidSchema("No connection adapters were found for "+url.url)
                    except Exception as e:
                        eh.handleError(log, exception=e, msg="doWork", exc_info=True, url=url.url,thread=current_thread())
                        self.handle_request(url, None, e)

                except Exception as e:
                    eh.handleError(log, exception=e, msg="uncaught in doWork",exc_info=True, url=url.url,thread=current_thread())

        log.info("Stopped", thread=current_thread())




    def handle_request(self, resource, response, error):
        with Timer(key="handle_request") as t:
            props={}
            props['mime']=None
            props['size']=None
            props['redirects']=None
            props['status']=None
            props['header']=None
            props['exception']=None
            props['timestamp'] = datetime.datetime.now()

            if error is not None:
                eh.handleError(log, "handle_request", exception=error, url=resource.url, snapshot=self.sn,exc_info=True)
                props['status']=util.getExceptionCode(error)
                props['exception']=util.getExceptionString(error)
            else:
                with Timer(key="header_dict") as t:
                    header_dict = dict((k.lower(), v) for k, v in dict(response.headers).iteritems())

                    if 'content-type' in header_dict:
                        props['mime']= util.extractMimeType(header_dict['content-type'])
                    else:
                        props['mime']='missing'

                    props['status']=response.status_code
                    props['header']=header_dict

                    if response.status_code == 200:
                        if 'content-length' in header_dict:
                            props['size']=header_dict['content-length']
                        else:
                            props['size']=0

            try:
                with Timer(key="updateResourceStatus"):
                    resource.updateStats(props)
                    self.dbm.updateResource(resource)

                    netloc="err"
                    try:
                        o = urlparse(resource.url)
                        netloc=o.netloc
                    except:
                        pass
                    for k in ['total',netloc]:
                        t=statuscount.setdefault(k,{})
                        if props['status'] not in t:
                            t[props['status']]=0
                        t[props['status']]+=1

                log.info("Processed", url=resource.url, status=resource.status,thread=current_thread())
            except Exception as e:
                eh.handleError(log, "Processed Exception", exception=e, url=resource.url, snapshot=self.sn,exc_info=True,thread=current_thread())

    def checkUpdateRobots(self,url):
        ttl = 36000
        with Timer(key="checkUpdateRobots"):
            log.info("Robots.txt", url=url.url,thread=current_thread())

            canonical = hostname(url.url)
            robots_url = roboturl(url.url)

            while canonical in self.robots.robotsInProgress:
                #seems one thread is doing the parsing already
                sleep(5)

            if canonical not in self.robots.robots._cache:
                self.robots.robotsInProgress.add(canonical)
                with Timer(key="robots.fetch_parse"):
                    try:
                        # First things first, fetch the thing
                        log.info('Fetching', url=robots_url)
                        req = self.rsession.get(robots_url, timeout=10, allow_redirects=True, headers=headers)


                        # And now parse the thing and update
                        import reppy.parser
                        r=reppy.parser.Rules(robots_url, req.status_code, req.content, time.time() + ttl)
                        self.robots.robots.add(r)
                        delay = self.robots.robots.delay(url.url, user_agent)
                        self.queue.addWait(url.url, delay)
                    except Exception as e:
                        with Timer(key="robots.allowed.error"):
                            eh.handleError(log,"Robots.txt Exception", exception=e, url=url.url,thread=current_thread())
                            try:
                                import reppy.parser
                                r= reppy.parser.Rules(robots_url, 499, "", time.time() + ttl)
                                self.robots.robots.add(r)
                            except Exception as e:
                                eh.handleError(log,"Robots.txt ExException", exception=e, url=url.url,thread=current_thread())
                self.robots.robotsInProgress.remove(canonical)

            return self.robots.robots.allowed( url.url, user_agent)

    @timelimit(30)
    def headlookup(self,ourl):
        with closing(self.rsession.head(url=ourl, timeout=5, allow_redirects=True, headers=headers)) as r:
            return r

    @timelimit(30)
    def getlookup(self,ourl):
        with closing(self.rsession.get(ourl, stream=True, timeout=5, allow_redirects=True, headers=getheaders)) as r:
            return r

    def getStatus(self,ourl):
        with Timer(key="getStatus"):
            with Timer(key="HEADLookup"):
                headResp=self.headlookup(ourl)
            if headResp.status_code==400:
                with Timer(key="GETLookup"):
                    headResp=self.getlookup(ourl)
            return headResp


class RobotsManager(object):
    def __init__(self, rsession):
        self.robots = RobotsCache(timeout=10, allow_redirects=True, session=rsession, headers=headers)
        self.robotsInProgress=set([])


done=[]
#def load(http_client, dbm, sn, status):

def hostname(url):
    '''Return a normalized, canonicalized version of the url's hostname'''
    return urlparse(url).netloc

def roboturl(url):
    '''Return a normalized uri to the robots.txt'''
    parsed = urlparse(url)
    return '%s://%s/robots.txt' % (parsed.scheme, parsed.netloc)









def cli(args,dbm):

    dbm.engine.dispose()
    sn = getSnapshot(args)
    if not sn:
        return

    batch=10
    concurrent = 30

    rsession= requests.Session()
    robots=RobotsManager(rsession)

    q = DomainQueue(5)
    log.info("Filling Queue", batch=batch)

    filler= QueueFiller(dbm,q, robots, batch, args.status, sn, concurrent)
    filler.daemon = True
    filler.filling_queue(status=args.status, batch=batch)


    #start worker threads
    for i in range(concurrent-1):
        t = Worker(dbm=dbm, q=q, robots=robots, rsession=rsession,sn=sn)
        t.daemon = True
        t.start()


    filler.start()
    filler.join()


    Timer.printStats()

    import sys
    sys.exit(0)