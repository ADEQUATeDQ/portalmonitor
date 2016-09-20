import structlog
import yaml
from tornado.ioloop import IOLoop
from tornado.web import FallbackHandler, RequestHandler, Application
from tornado.wsgi import WSGIContainer

from odpw.web_rest.rest.portal_namespace import ns as portal_namespace
from odpw.web_rest.rest.portals_namespace import ns as portals_namespace
from odpw.web_rest.rest.odpw_restapi import api
from odpw.core.db import DBManager
from odpw.core.api import DBClient

from odpw.web_rest.cache import cache
log =structlog.get_logger()

from flask import Flask, jsonify, request, Blueprint

# database session registry object, configured from
# create_app factory
from flask import _app_ctx_stack
from sqlalchemy.orm import scoped_session, sessionmaker





class MainHandler(RequestHandler):
    def get(self):
      self.write("This message comes from Tornado ^_^")


DbSession = scoped_session(
    sessionmaker(),
    # __ident_func__ should be hashable, therefore used
    # for recognizing different incoming requests
    scopefunc=_app_ctx_stack.__ident_func__
)

def create_app(dbm,conf):
    """
    Application factory

    :param name_handler: name of the application.
    :param config_object: the configuration object.
    """

    app = Flask(__name__)

    #app.config.from_object(config_object)
    app.engine = dbm.engine
    cache.init_app(app)

    dbc= DBClient(dbm)

    app.config['dbsession']=dbc.Session
    app.config['dbc']=dbc


    @app.teardown_appcontext
    def teardown(exception=None):
        #print 'tear down',app.config['dbsession']
        if app.config['dbsession']:
            app.config['dbsession'].remove()

    @app.errorhandler(500)
    def internal_error(e):
        app.logger.error(e)
        message = {
            'status': 500,
            'exception': e.message,
            'request_url': request.url
        }
        resp = jsonify(message)
        resp.status_code = 500
        return resp

    blueprint = Blueprint('api', __name__, url_prefix=conf['url_prefix'])
    api.init_app(blueprint)
    api.add_namespace(portal_namespace)
    api.add_namespace(portals_namespace)
    app.register_blueprint(blueprint)

    return app

def name():
    return 'RestAPI'

def help():
    return "ODPW restful api"

def setupCLI(pa):
    pass

def cli(args,dbm):

    conf={
        'url_prefix':'api'
        ,'port':5122
    }
    if args.config:
        with open(args.config) as f_conf:
            config = yaml.load(f_conf)
            if 'rest' in config:
                for key in conf:
                    if key in config['rest']:
                        conf[key]=config['rest'][key]


    app=create_app(dbm,conf)

    tr = WSGIContainer(app)

    application = Application([
        (r"/tornado", MainHandler),
        (r".*", FallbackHandler, dict(fallback=tr)),
    ])
    application.listen(conf['port'])
    print "Listinging on http://localhost:"+str(conf['port'])+"/"+conf['url_prefix']
    log.info("Server running", url="http://localhost:"+str(conf['port'])+"/"+conf['url_prefix'])
    IOLoop.instance().start()
