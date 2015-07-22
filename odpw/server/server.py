# import Jinja2
from jinja2 import Environment, FileSystemLoader

import sys


from tornado.web import Application, StaticFileHandler
import tornado.httpserver
import tornado.ioloop


from os.path import dirname, join, isfile


here = dirname(__file__)
project_root = join(here, '..')

from handler import IndexHandler, NoDestinationHandler, DataHandler,PortalList,PortalHandler

# # List for famous movie rendering
# movie_list = [[1,"The Hitchhiker's Guide to the Galaxy"],[2,"Back to future"],[3,"Matrix"]]

class Application(tornado.web.Application):
    def __init__(self, db=None):
        
        static_path = join(here, 'static')
        template_path = join(here, 'templates')
        handlers = [
            (r'/', IndexHandler),
            (r'/list/portals', PortalList),
            (r'/portal/', PortalHandler),
            (r'/static/(.*)', StaticFileHandler),
            (r'/data/(.*)/(.*)', DataHandler),
            (r'/.*$', NoDestinationHandler)
        ]

        settings = {
            'debug': isfile(join(project_root, 'debug')),
            'static_path': static_path,
            'autoreload':True
        }

        super(Application, self).__init__(handlers, **settings)
        
        self.env = Environment(loader=FileSystemLoader(template_path))
        self.db = db



def name():
    return 'Server'

def setupCLI(pa):
    pa.add_argument('-p','--port',type=int, dest='port', default=2340)    

def cli(args,dbm):
    http_server = tornado.httpserver.HTTPServer(Application(db=dbm))
    http_server.listen(args.port)
    
    print('Starting Tornado on http://localhost:{}/'.format(args.port))
    try:
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        sys.exit(0)
    
        
    