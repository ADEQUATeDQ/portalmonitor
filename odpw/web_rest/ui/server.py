import structlog
from werkzeug.utils import redirect

from odpw.web_rest.rest.portal_namespace import ns as portal_namespace
from odpw.web_rest.rest.portals_namespace import ns as portals_namespace
from odpw.web_rest.rest.datamonitor_namespace import ns as datamonitor_namespace
from odpw.web_rest.rest.odpw_restapi import api

log =structlog.get_logger()

import yaml
from sqlalchemy.orm import scoped_session, sessionmaker
from tornado.ioloop import IOLoop
from tornado.web import FallbackHandler, RequestHandler, Application
from tornado.wsgi import WSGIContainer

from odpw.web_rest.cache import cache
from odpw.web_rest.ui.odpw_ui_blueprint import ui

from odpw.core.api import DBClient
from flask import Flask, jsonify, request, Blueprint


class ReverseProxied(object):
    '''Wrap the application in this middleware and configure the
    front-end server to add these headers, to let you quietly bind
    this to a URL other than / and to an HTTP scheme that is
    different than what is used locally.

    In nginx:
    location /myprefix {
        proxy_pass http://192.168.0.1:5001;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Scheme $scheme;
        proxy_set_header X-Script-Name /myprefix;
        }

    :param app: the WSGI application
    '''
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        script_name = environ.get('HTTP_X_SCRIPT_NAME', '')
        if script_name:
            environ['SCRIPT_NAME'] = script_name
            path_info = environ['PATH_INFO']
            if path_info.startswith(script_name):
                environ['PATH_INFO'] = path_info[len(script_name):]

        scheme = environ.get('HTTP_X_SCHEME', '')
        if scheme:
            environ['wsgi.url_scheme'] = scheme
        return self.app(environ, start_response)


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
    api.add_namespace(datamonitor_namespace)

    app.register_blueprint(blueprint)

    #cors = CORS(app, resources={r"/v1/*": {"origins": "*"}})

    @app.after_request
    def after_request(response):
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
        response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
        return response

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

    @app.route('/')
    def url_prefix():
        return redirect(conf['url_prefix_ui'])


    app.wsgi_app = ReverseProxied(app.wsgi_app)
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
        ,'port':80
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
                        conf[key]=config['rest'][key]

    print conf
    app=create_app(dbm, conf)

    tr = WSGIContainer(app)

    #application = Application([
    #    (r"/tornado", MainHandler),
    #    (r".*", FallbackHandler, dict(fallback=tr)),
    #])
    #application.listen(conf['port'])
    print "Listinging on ui: http://localhost:"+str(conf['port'])+""+conf['url_prefix_ui'], "api: http://localhost:"+str(conf['port'])+""+conf['url_prefix_rest']
    log.info("Server running", ui="http://localhost:"+str(conf['port'])+""+conf['url_prefix_ui'],api="http://localhost:"+str(conf['port'])+""+conf['url_prefix_rest'])
    app.run(threaded=True, port=conf['port'], host='0.0.0.0')
    #IOLoop.instance().start()
