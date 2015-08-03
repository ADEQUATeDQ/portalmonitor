import json
__author__ = 'jumbrich'


from odpw.db.models import Portal
from odpw.utils.util import ErrorHandler as eh

import structlog
log =structlog.get_logger()


def name():
    return 'Init'
def help():
    return "Init the system with portals"

def setupCLI(pa):
    pa.add_argument('-p','--portals',type=file, dest='plist')
    pa.add_argument('-i','--insert',action='store_true', dest='insert')
    pa.add_argument('-u','--update',action='store_true', dest='update')
    #pa.add_argument('-s','--software', dest='software', default='CKAN')

def cli(args,dbm):
    if args.plist:
        if args.insert or args.update:
            ok=0
            fail=0
            
            data = json.load(args.plist)
            
            for k,v in data.items():
                
                pid = k
                url = v['url']
                apiurl = v['api'] if len(v['api'].strip())>0 else url
                software = v['software']
                iso3 = v['countryCode']
                try:
                    if args.insert:
                        p = dbm.getPortal(portalID=pid)
                        if p:
                            print "Portal", p.url, "exists"
                            fail+=1
                        else:
                            p = Portal.newInstance(pid=pid,url=url, apiurl=apiurl,software=software, iso3=iso3)
                            dbm.insertPortal(p)
                            ok+=1
                
                    #elif args.update:
                    #    p = dbm.getPortal(url=url)
                    #    pn= Portal.newInstance(url=url, apiurl=apiurl,software=software, iso3=iso3)
                    #    if p:
                    #        p.apiurl=pn.apiurl
                    #        p.exception = pn.exception
                    #        dbm.updatePortal(p)
                    #    else:
                    #        dbm.insertPortal(pn)



                except Exception as e:
                    print e
                    eh.handleError(log, "Insert new Portal", exception=e, line=k,exc_info=True)
                    #log.error("Insert new Portal", line=l, exctype=type(e), excmsg=e.message,exc_info=True)
                    fail+=1
            log.info("InitPortals DONE", total=(ok+fail), ok=ok, failed=fail)