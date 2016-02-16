'''
Created on Aug 21, 2015

@author: jumbrich
'''

from datetime import datetime, timedelta
from pytz import timezone
import pytz
import dateutil


if __name__ == '__main__':
    unaware = datetime(2011,8,15,8,15,12,0)
    aware = datetime(2011,8,15,8,15,12,0,pytz.UTC)
    print aware
    if not aware.tzinfo:
        print  pytz.utc.localize(aware)
    now_aware = pytz.utc.localize(unaware)
    print now_aware > aware
    #import xml.utils.iso8601
    s='2015-08-21T14:01:24.198798Z'
    #print xml.utils.iso8601.parse(s)
    print datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")
    s="Tue, 18 Aug 2015 20:51:01 GMT, Tue, 18 Aug 2015 20:51:01 GMT"
    
    
    span = 2
    
    import dateutil.parser
    if s.count(",") == 3:
        words = s.split(",")
        ss=  [",".join(words[i:i+span]) for i in range(0, len(words), span)]
        print dateutil.parser.parse(ss[0])
        i= s.replace(',', 'XXX', 1).find(',')
        
    
    t = s.split(",")
    tt =[ a.strip() for a in t]
    print t
    print set(tt)
    print len(tt), len(set(tt))
    
    