# -*- coding: utf-8 -*-


__author__ = 'jumbrich'



import structlog
log = structlog.get_logger()


import datetime
import requests
from requests.exceptions import InvalidSchema

from time import sleep
from reppy.cache import RobotsCache
import threading
from threading import  current_thread
from urlparse import urlparse
import time
from contextlib import closing

from Queue import Queue

from odpw.core.api import DBClient
from odpw.utils.timing import Timer,timer
from odpw.utils.utils_snapshot import getCurrentSnapshot
from odpw.utils.error_handling import TimeoutError, ErrorHandler, getExceptionCode, getExceptionString
from odpw.core.model import ResourceInfo
from odpw.utils.helper_functions import extractMimeType



user_agent='OpenDataPortalWatch (+http://data.wu.ac.at/portalwatch/about)'
headers = {
    'User-Agent': user_agent,
    'From': 'contact@data.wu.ac.at'  # This is another valid field
}
getheaders={"range": "bytes=3-5",'Accept-Encoding': 'identity'}.update(headers)

def resourceMigrate(snapshot, db, dbm):

    from odpw.db.models import Resource
    iter=Resource.iter(dbm.getResources(snapshot=snapshot))

    batch=[]
    for R in iter:
        uri=R.url
        uri=uri.replace("http:// \thttp:","http:")
        uri=uri.replace("http:// http://","http://")

        r={ 'snapshot':R.snapshot ,'uri':uri
            ,'timestamp':R.timestamp ,'status':R.status
            ,'exc':R.exception ,'header':R.header
            ,'mime':R.mime ,'size':R.size
          }
        RI=ResourceInfo(**r)

        if not db.exist_resourceinfo(RI.uri, RI.snapshot):
            if db.exist_metaresource(RI.uri):
                batch.append(RI)
                print len(batch)
            else:
                log.warn("URI missing", uri=RI.uri)
                print R.url, uri


        if len(batch)==1000:
            log.info("BatchInsert", size=len(batch))
            with Timer(key="BatchInsert", verbose=True):
                db.bulkadd(batch)
            batch=[]

    log.info("BatchInsert", size=len(batch))
    db.bulkadd(batch)

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
                e=TimeoutError(message="Processing"+str(function),timeout=timeout)
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
        self.isDone=False

    def get(self):
        while True:
            if self.size()==0 and self.isDone:
                log.info("Queue is empty!!!")
            elif self.size()==0 and not self.isDone:
                log.info("Queue is empty but not done!!!")

            for d,v in self.queue.items():
                if len(v['urls'])!=0:
                    now = datetime.datetime.now()
                    if now - v['last'] > datetime.timedelta(seconds=self.waits[d]):
                        v['last']=now
                        if len(v['urls']) != 0:
                            uri= v['urls'].pop()
                            self.seen.add(uri)
                            return uri
            sleep(self.wait/2.0)

    def done(self):
        self.isDone=True

    def addWait(self, url, wait):
        try:
            o = urlparse(url)
            loc=o.netloc
            delay = wait if wait is not None else self.wait
            if loc not in self.waits:
                log.info("UPDATE DELAY INFO", domain=loc, delay=delay)
                self.waits[loc]=delay
            elif delay != self.waits[loc]:
                log.info("UPDATE DELAY INFO", domain=loc, delay=delay)
                self.waits[loc]=delay
        except Exception as e:
            pass

    def put(self, uri):
        try:
            o = urlparse(uri)
            loc=o.netloc
            if loc not in self.waits:
                self.addWait(uri,None)
            if uri not in self.seen:
                d= self.queue.setdefault(loc,{'last':datetime.datetime.now(), 'urls':set([])})
                if uri not in d['urls']:
                    d['urls'].add(uri)
                    log.info("ADD2QUEUE", uri=uri)
                    return True
        except Exception as e:
            ErrorHandler.handleError(log,"ADD2QUEUE", exception=e, url=uri)
            pass
        return False

    def urlsSeen(self):
        return len(self.seen)

    def info(self):
        l= "<QUEUE( size:"+str(self.size())+", domains:"+str(self.domains())+", seen:"+str(self.urlsSeen())+") ["
        for k,v in self.queue.items():
            l +='('+k.encode('utf-8')+","+str(len(v['urls']))+','+str(self.waits[k] if k in self.waits else "nys")+','+v['last'].isoformat()+')'
        l += ']>'
        print l


    def __str__(self):
        return "<QUEUE (size:"+str(self.size())+", domains:"+str(self.domains())+", seen:"+str(len(self.seen))+")>"


    def __repr__(self):
        return self.__str__()

    def domains(self):
        n=0
        for d, s in self.queue.iteritems():
            if len(s['urls'])>=0:
                n+=1
        return n

    def size(self):
        c=0
        for k,v in self.queue.items():
            c+=len(v['urls'])
        return c

statuscount={'total':{}}

from threading import Thread

class QueueFiller(Thread):
    def __init__(self, db,q, robots, batch,  sn, concurrent):
        Thread.__init__(self)

        self.db=db
        self.queue=q
        self.batch=batch
        self.robots=robots
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
            log.info("QueueInfo", queue=str(self.queue), fill=fill, size=size, domain=domain)

            if fill:
                self.filling_queue(self.batch)
                #self.statusInfo()

            if self.queue.size()==0:
                break

            c+=1
            sleep(10)
            if c % (100) == 0:
                self.statusInfo()

        log.info("FILLTHREAD stopping")
        self.queue.done()

    def statusInfo(self):

        seen= self.queue.urlsSeen()
        now=time.time()

        elapsed = (now - self.starttime)
        interim=(now-self.interim)

        s=[ "SYSTEM STATUS","\n"
            ,str(self.queue)
            ," Robots",str(len(self.robots.robots._cache.keys())),"\n"
            ," active_threads",str(threading.activeCount()),"\n"
            ," statuscount_total",str(statuscount['total']),"\n"
            ," statuscount",str(statuscount),"\n"
            ," -------------------------","\n"
            ," Running since:", timer(elapsed),"\n"
            ,"   processed:", str(seen),"\n"
            ,"   performance:", str(seen/(1.0*elapsed))," urls/sec","\n"
            ," since_last_update:", timer(interim),"\n"
            ,"   processed:", str(seen-self.lastSeen),"\n"
            ,"   performance:", str((seen-self.lastSeen)/(1.0*interim))," urls/sec","\n"
            ," -------------------------","\n"
            ," Timing stats:","\n"
        ]

        for m,st in Timer.measures.items():
            d=[    "  ["+m+'] -', str(st.mean),'avg ms for',m,str(st.n),'calls)'
                   ,"\n       (min:",str(st.min),"-",str(st.max)," max, rep:",st.__repr__(),")\n"]
            s=s+d



        s.append(" -------------------------\n")
        s.append(" Exceptions\n")
        for exc, count in ErrorHandler.exceptions.iteritems():
            s.append("  ")
            s.append(exc)
            s.append(str(count))
            s.append("\n")

        print " ".join(s)

        log.info("STATUS", q=str(self.queue), robots=len(self.robots.robots._cache.keys()), active=threading.activeCount())

        self.interim=now
        self.lastSeen=seen

    def filling_queue(self, batch):
        with Timer(key='filling_queue'):
            log.info("FILLING QUEUE", batch=batch)

            uris =[ uri[0] for uri in self.db.getUnfetchedResources(self.snapshot, batch=batch) ]

            c={'New':0, 'Exists':0}
            for uri in uris:
                t=self.queue.put(uri)
                if t:
                    c['New']+=1
                else:
                    c['Exists']+=1
            log.info("FILLED QUEUE", status=c, queue=self.queue)

class Inserter(Thread):
    def __init__(self, db, resultQueue, domainQueue, batch=1000):
        Thread.__init__(self)

        self.db=db
        self.domainQueue= domainQueue
        self.batch=batch
        self.resultQueue=resultQueue


    def run(self):
        log.info("INSERTER STARTED")
        batch=[]

        while not self.domainQueue.isDone or self.domainQueue.size() > 0 :
            while not self.resultQueue.empty():
                RI=self.resultQueue.get()
                self.resultQueue.task_done()
                batch.append(RI)

                if len(batch)==self.batch:
                    log.info("BATCH_INSERT", size=len(batch))
                    with Timer(key="BatchInsert", verbose=True):
                        self.db.bulkadd(batch)
                        batch=[]
            sleep(2)

        log.info("BATCH_INSERT", size=len(batch))
        self.db.bulkadd(batch)


class Worker(Thread):
    def __init__(self, q, resultQueue, sn, robots, rsession):
        Thread.__init__(self)

        self.queue=q

        self.resultQueue=resultQueue
        self.snapshot=sn
        self.robots=robots
        self.rsession=rsession

    def run(self):
        log.info("Started", thread=current_thread())
        while True:
            with Timer(key="doWork"):
                try:
                    with Timer(key="q.get"):
                        uri = self.queue.get()
                    if uri is None:
                        break
                    try:
                        #check if this is a valid URL, if we have an exception, done
                        o = urlparse(uri)
                        if o.scheme.startswith('http'):
                            res=None
                            if self.checkUpdateRobots(uri):
                                res = self.getStatus(uri)
                            else:
                                log.info("ROBOTS DENIED", uri=uri, thread=current_thread())
                            self.handle_request(uri, res, None)
                        else:
                            raise InvalidSchema("No connection adapters were found for "+uri)
                    except Exception as e:
                        ErrorHandler.handleError(log, exception=e, msg="doWork", exc_info=True, url=uri,thread=current_thread())
                        self.handle_request(uri, None, e)

                except Exception as e:
                    ErrorHandler.handleError(log, exception=e, msg="uncaught in doWork",exc_info=True,thread=current_thread())

        log.info("STOPPED", thread=current_thread())

    def handle_request(self, uri, response, error):
        with Timer(key="handle_request") as t:
            try:
                r={
                    'snapshot':self.snapshot
                    ,'uri':uri
                    ,'timestamp':datetime.datetime.now()
                    ,'status':None
                    ,'exc':None
                    ,'header':None
                    ,'mime':None
                    ,'size':None
                }

                #robots
                if response is None and error is None:
                    r['status']=666
                elif error is not None:
                    ErrorHandler.handleError(log, "handle_request", exception=error, url=uri, snapshot=self.snapshot,exc_info=True)
                    r['status']=getExceptionCode(error)
                    r['exc']=getExceptionString(error)
                else:
                    with Timer(key="header_dict") as t:
                        header_dict = dict((k.lower(), v) for k, v in dict(response.headers).iteritems())

                        if 'content-type' in header_dict:
                            r['mime']= extractMimeType(header_dict['content-type'])
                        else:
                            r['mime']='missing'

                        r['status']=response.status_code
                        r['header']=header_dict

                        if response.status_code == 200:
                            if 'content-length' in header_dict:
                                r['size']=header_dict['content-length']
                            else:
                                r['size']=0



                RI=ResourceInfo(**r)
                self.resultQueue.put(RI)
                log.info("PROCESSED", uri=uri, status=RI.status,thread=current_thread())
            except Exception as e:
                ErrorHandler.handleError(log, "Processed Exception", exception=e, url=uri, snapshot=self.snapshot,exc_info=True,thread=current_thread())

    def checkUpdateRobots(self,uri):
        ttl = 36000
        with Timer(key="checkUpdateRobots"):
            log.info("Robots.txt", url=uri,thread=current_thread())

            canonical = hostname(uri)
            robots_url = roboturl(uri)

            while canonical in self.robots.robotsInProgress:
                #seems one thread is doing the parsing already
                sleep(5)

            if canonical not in self.robots.robots._cache:
                self.robots.robotsInProgress.add(canonical)
                with Timer(key="robots.fetch_parse"):
                    try:
                        # First things first, fetch the thing
                        log.info('HTTP_GET', uri=robots_url)
                        req = self.rsession.get(robots_url, timeout=10, allow_redirects=True, headers=headers)

                        # And now parse the thing and update
                        import reppy.parser
                        r=reppy.parser.Rules(robots_url, req.status_code, req.content, time.time() + ttl)
                        self.robots.robots.add(r)
                        delay = self.robots.robots.delay(uri, user_agent)
                        self.queue.addWait(uri, delay)
                    except Exception as e:
                        with Timer(key="robots.allowed.error"):
                            ErrorHandler.handleError(log,"Robots.txt Exception", exception=e, url=uri,thread=current_thread())
                            try:
                                import reppy.parser
                                r= reppy.parser.Rules(robots_url, 499, "", time.time() + ttl)
                                self.robots.robots.add(r)
                            except Exception as e:
                                ErrorHandler.handleError(log,"Robots.txt ExException", exception=e, url=uri,thread=current_thread())
                self.robots.robotsInProgress.remove(canonical)
            return self.robots.robots.allowed( uri, user_agent)

    @timelimit(30)
    def headlookup(self,ourl):
        with closing(requests.head(url=ourl, timeout=5, allow_redirects=True, headers=headers)) as r:
            return r

    @timelimit(30)
    def getlookup(self,ourl):
        with closing(self.rsession.get(ourl, stream=True, timeout=5, allow_redirects=True, headers=getheaders)) as r:
            return r

    def getStatus(self, uri):
        with Timer(key="getStatus"):
            with Timer(key="HEADLookup"):
                headResp=self.headlookup(uri)
            if headResp.status_code==400:
                with Timer(key="GETLookup"):
                    headResp=self.getlookup(uri)
            return headResp


class RobotsManager(object):
    def __init__(self, rsession):
        self.robots = RobotsCache(timeout=10, allow_redirects=True, session=rsession, headers=headers)
        self.robotsInProgress=set([])


done=[]

def hostname(url):
    '''Return a normalized, canonicalized version of the url's hostname'''
    return urlparse(url).netloc

def roboturl(url):
    '''Return a normalized uri to the robots.txt'''
    parsed = urlparse(url)
    return '%s://%s/robots.txt' % (parsed.scheme, parsed.netloc)


def help():
    return "perform head lookups"
def name():
    return 'Head'

def setupCLI(pa):
    pa.add_argument("-t","--threads",  help='Number of threads',  dest='threads', default=4,type=int)
    pa.add_argument("-b","--batch",  help='Batch size',  dest='batch', default=100,type=int)
    pa.add_argument("-d","--delay",  help='Default domain delay (in sec)',  dest='delay', default=5,type=int)

def cli(args,dbm):

    sn = getCurrentSnapshot()
    db=DBClient(dbm)


    batch=args.batch
    concurrent = args.threads
    delay= args.delay

    log.info("START HEAD", batch=batch, delay=delay, threads=concurrent)

    rsession= requests.Session()
    robots=RobotsManager(rsession)

    q = DomainQueue(args.delay)

    filler = QueueFiller(db, q, robots, batch*2, sn, concurrent)
    filler.daemon = True
    filler.filling_queue(batch=batch)


    resultQueue = Queue(maxsize=0)
    #start worker threads
    for i in range(concurrent):
        t = Worker(q=q, resultQueue=resultQueue, robots=robots, rsession=rsession,sn=sn)
        t.daemon = True
        t.start()

    filler.start()

    inserter= Inserter(db=db, resultQueue=resultQueue, domainQueue=q,batch=batch/2)
    inserter.start()

    filler.join()
    inserter.join()
    Timer.printStats()

    import sys
    sys.exit(0)