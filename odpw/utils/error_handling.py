import traceback
from ast import literal_eval
from collections import defaultdict

import ckanapi
import exceptions
import requests
import urlnorm

import structlog

log =structlog.get_logger()



class TimeoutError(Exception):
    def __init__(self, message, timeout):
        # Call the base class constructor with the parameters it needs
        super(TimeoutError, self).__init__(message)

        # Now for your custom code...
        self.timeout = timeout



class ErrorHandler():

    exceptions=defaultdict(long)

    DEBUG=False

    @classmethod
    def handleError(cls, log, msg=None, exception=None, debug=False, **kwargs):
        name=type(exception).__name__
        cls.exceptions[name] +=1

        if debug:
            print(traceback.format_exc())

        log.error(msg, exctype=type(exception), excmsg=exception.message, **kwargs)

    @classmethod
    def printStats(cls):
        print '>>>','--*'*10,'EXCEPTIONS','*--'*10
        if len(cls.exceptions)==0:
            print "No exceptions handled"
        else:
            print "  Numbers of Exceptions:"
            for exc, count in cls.exceptions.iteritems():
                print " ",exc, count
        print '<<<','--*'*25



errorStatus={
    702:'Connection Error'
    ,703:'Connection Timeout'
    ,704:'Read Timeout'

    ,705:'HTTPError'
    ,706:'TooManyRedirects'
    ,707:'Timeout'
    ,801:'ValueError'
    ,802:'TimeoutError'
    ,901:'InvalidUrl'

    ,902:'InvalidSchema'
    ,903:'MissingSchema'
    ,904:'MissingDatasets'

    ,600:'Not Specified'
    ,666:'Robots.txt'
}


def getExceptionCode(e):
    #connection erorrs
    try:

        if isinstance(e,requests.exceptions.ConnectionError):
            return 702
        if isinstance(e,requests.exceptions.ConnectTimeout):
            return 703
        if isinstance(e,requests.exceptions.ReadTimeout):
            return 704
        if isinstance(e,requests.exceptions.HTTPError):
            return 705
        if isinstance(e,requests.exceptions.TooManyRedirects):
            return 706
        if isinstance(e,requests.exceptions.Timeout):
            return 707
        if isinstance(e,ckanapi.errors.CKANAPIError):
            try:
                err = literal_eval(e.extra_msg)
                return err[1]
            except Exception:
                return 708

        #if isinstance(e,requests.exceptions.RetryError):
        #    return 708

        #parser errors
        if isinstance(e, exceptions.ValueError):
            return 801
        if isinstance(e , TimeoutError):
            return 802
        #format errors
        if isinstance(e,urlnorm.InvalidUrl):
            return 901
        if isinstance(e,requests.exceptions.InvalidSchema):
            return 902
        if isinstance(e,requests.exceptions.MissingSchema):
            return 903
        else:
            return 600
    except Exception as e:
        log.error("MISSING Exception CODE", exctype=type(e), excmsg=e.message,exc_info=True)
        return 601

def getExceptionString(e):
    try:
        if isinstance(e,ckanapi.errors.CKANAPIError):
            try:
                err = literal_eval(e.extra_msg)
                return str(type(e))+":"+str(err[2])
            except Exception:
                return str(type(e))+":"+str(e.extra_msg)
        else:
            if e.message:
                return str(type(e))+":"+str(e.message)
            if e.message:
                return str(type(e))+":"
    except Exception as e:
        log.error("Get Exception string", exctype=type(e), excmsg=e.message,exc_info=True)
        return 601