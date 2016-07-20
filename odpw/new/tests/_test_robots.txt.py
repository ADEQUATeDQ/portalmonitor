import requests

from new.services.resource_head import RobotsManager, Worker, DomainQueue

rsession= requests.Session()
robots=RobotsManager(rsession)

q = DomainQueue(5)
t = Worker(q=q, resultQueue=None, robots=robots, rsession=rsession,sn=None)

print t.checkUpdateRobots('http://data.wu.ac.at/dataset/679a9782-736e-44fd-92ea-a1048d91f3a9/resource/4cd064c2-6806-4755-a522-305bf1585a1d/download/allcoursesandorgid16s.csv')

