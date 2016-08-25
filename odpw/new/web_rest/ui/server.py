import structlog

from odpw.new.web_rest.rest.portal_namespace import ns as portal_namespace
from odpw.new.web_rest.rest.portals_namespace import ns as portals_namespace
from odpw.new.web_rest.rest.odpw_restapi import api

log =structlog.get_logger()

import yaml
from sqlalchemy.orm import scoped_session, sessionmaker
from tornado.ioloop import IOLoop
from tornado.web import FallbackHandler, RequestHandler, Application
from tornado.wsgi import WSGIContainer

from odpw.new.web_rest.cache import cache
from odpw.new.web_rest.ui.odpw_ui_blueprint import ui

from odpw.new.core.api import DBClient
from flask import Flask, jsonify, request, Blueprint


class MainHandler(RequestHandler):
    def get(self):
      self.write("This message comes from Tornado ^_^")

def create_app(dbm, conf):
    """
    Application factory

    :param name_handler: name of the application.
    :param config_object: the configuration object.
    """



    app = Flask(__name__)

    #app.config.from_object(config_object)
    app.engine = dbm.engine
    cache.init_app(app)


    DbSession = scoped_session(sessionmaker(
                                         bind=dbm.engine
                                        ))
    app.config['dbsession']=DbSession
    app.config['dbc']= DBClient(dbm)

    app.register_blueprint(ui, url_prefix=conf['url_prefix_ui'])
    blueprint = Blueprint('api', __name__, url_prefix=conf['url_prefix_rest'])
    api.init_app(blueprint)
    api.add_namespace(portal_namespace)
    api.add_namespace(portals_namespace)
    app.register_blueprint(blueprint)

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
    return app


def name():
    return 'ODPWUI'

def help():
    return "ODPW UI"

def setupCLI(pa):
    pass

def cli(args,dbm):

    conf={
        'url_prefix_rest':'/api',
         'url_prefix_ui':'/ui'
        ,'port':5123
    }
    if args.config:
        with open(args.config) as f_conf:
            config = yaml.load(f_conf)
            if 'ui' in config:
                for key in conf:
                    if key in config['ui']:
                        conf[key]=config['ui'][key]
            if 'rest' in config:
                for key in conf:
                    if key in config['rest']:
                        conf[key]=config['ui'][key]

    print conf
    app=create_app(dbm, conf)

    tr = WSGIContainer(app)

    #application = Application([
    #    (r"/tornado", MainHandler),
    #    (r".*", FallbackHandler, dict(fallback=tr)),
    #])
    #application.listen(conf['port'])
    print "Listinging on ui: http://localhost:"+str(conf['port'])+"/"+conf['url_prefix_ui'], "api: http://localhost:"+str(conf['port'])+"/"+conf['url_prefix_rest']
    log.info("Server running", ui="http://localhost:"+str(conf['port'])+"/"+conf['url_prefix_ui'],api="http://localhost:"+str(conf['port'])+"/"+conf['url_prefix_rest'])
    app.run(threaded=True, port=conf['port'])
    #IOLoop.instance().start()
