import requests
from reppy.cache import RobotsCache

from odpw.utils.timer import Timer

rsession= requests.Session()
robots = RobotsCache(timeout=10, allow_redirects=True, session=rsession)



urls=['http://polleres.net/','http://wu.ac.at']


for i in range(1,10):
    for url in urls:
        with Timer(key="t") as t:
            print url, robots.allowed(url,'*')


Timer.printStats()