# import Jinja2
from jinja2 import Environment, FileSystemLoader,Markup

import sys
import json

from tornado.web import  StaticFileHandler, url
import tornado.httpserver
import tornado.ioloop


from os.path import dirname, join, isfile
from odpw.server.handler import SystemActivityHandler, PortalSelectionHandler


here = dirname(__file__)
project_root = join(here, '..')

from handler import IndexHandler, NoDestinationHandler, DataHandler,PortalList,PortalHandler

# # List for famous movie rendering
# movie_list = [[1,"The Hitchhiker's Guide to the Galaxy"],[2,"Back to future"],[3,"Matrix"]]

def tojson_filter(obj, **kwargs):
    # https://github.com/mitsuhiko/flask/blob/master/flask/json.py
    return Markup(json.dumps(obj, **kwargs))

class ODPWApplication(tornado.web.Application):
    def __init__(self, db=None, printHtml=False):
        
        static_path = join(here, 'static')
        template_path = join(here, 'templates')
        handlers = [
            (r'/', IndexHandler),
            
            (r'/activity/?(?P<snapshot>[^\/]+)?',SystemActivityHandler),
            (r'/portals', PortalList),
            
            #(r'/portal/?', PortalSelectionHandler),
            
            url(r'/portal/?(?P<portalID>[^\/]+)?/?(?P<view>[info|details|activity|quality|evolution]+)?/?(?P<snapshot>[^\/]+)?', PortalHandler),
            
            (r'/static/(.*)', StaticFileHandler),
            (r'/data/(.*)/(.*)', DataHandler),
            (r'/.*$', NoDestinationHandler)
        ]

        settings = {
            'debug': isfile(join(project_root, 'debug')),
            'static_path': static_path,
            'autoreload':True
        }

        super(ODPWApplication, self).__init__(handlers, **settings)
        
        self.env = Environment(loader=FileSystemLoader(template_path))
        self.env.filters.update( tojson= tojson_filter)
        
        self.db = db
        
        if printHtml:
            self.printHtml = static_path
        


def name():
    return 'Dashboard'
def help():
    return "Start the dashboard"

def setupCLI(pa):
    pa.add_argument('-p','--port',type=int, dest='port', default=2340)    

def cli(args,dbm):
    http_server = tornado.httpserver.HTTPServer(ODPWApplication(db=dbm, printHtml=True))
    http_server.listen(args.port)
    
    print('Starting Tornado on http://localhost:{}/'.format(args.port))
    try:
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        sys.exit(0)
    
        
    