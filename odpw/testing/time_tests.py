'''
Created on Jan 22, 2016

@author: jumbrich
'''
from datetime import date, datetime, timedelta


today = datetime.now()
start = today + timedelta(hours=2)
#nextCrawl = nextCrawl.replace(hour=0, minute=0, second=0, microsecond=0)

#start = today + timedelta((6 - today.weekday()) % 7)
start = start.replace( minute=0, second=0, microsecond=0)

end = start + timedelta(days=3)


print start, end