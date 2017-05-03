import argparse
import logging.config
import os

import structlog
import time

import sys

import odpw
from odpw.utils.timing import Timer
from odpw.utils.error_handling import ErrorHandler
from odpw_restapi import api, conv, qa

log =structlog.get_logger()

import yaml
from tornado.web import RequestHandler
from tornado.wsgi import WSGIContainer

from flask import Flask, jsonify, request, Blueprint


def config_logging():

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt='iso'),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(sort_keys=True)
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
    )



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

def create_app(conf):
    """
    Application factory

    :param name_handler: name of the application.
    :param config_object: the configuration object.
    """
    log.info("Setting up Flask")
    app = Flask(__name__)

    blueprint = Blueprint('api',
                          __name__,
                          url_prefix=conf['url_prefix_rest'],
                          template_folder='templates',
                          static_folder='static'
                          )

    api.init_app(blueprint)
    api.add_namespace(conv)
    api.add_namespace(qa)
    app.register_blueprint(blueprint)

    @app.after_request
    def after_request(response):
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
        response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
        return response

    @app.errorhandler(404)
    def page_not_found(error):
        app.logger.error('Page not found: %s', (request.path))
        message = {
            'status': 404,
            'path': request.path
        }
        resp = jsonify(message)
        resp.status_code = 404
        return resp

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

    @app.errorhandler(Exception)
    def unhandled_exception(e):
        import traceback
        traceback.print_exc()
        app.logger.error('Unhandled Exception: %s', (e))
        message = {
            'status': 500,
            'exception': e.message,
            'request_url': request.url
        }
        resp = jsonify(message)
        resp.status_code = 500
        return resp

    app.wsgi_app = ReverseProxied(app.wsgi_app)
    return app


def cli(args):
    conf = {
        'url_prefix_rest': '/api',
        'port': 8090
    }
    log.info("parsing configs for server")
    if args.config:
        with open(args.config) as f_conf:
            config = yaml.load(f_conf)
            if 'rest' in config:
                for key in conf:
                    if key in config['rest']:
                        conf[key]=config['rest'][key]

    print conf
    app=create_app(conf)

    tr = WSGIContainer(app)

    print "Listinging on api: http://localhost:"+str(conf['port'])+""+conf['url_prefix_rest']
    log.info("Server running", api="http://localhost:"+str(conf['port'])+""+conf['url_prefix_rest'])
    app.run(threaded=True, port=conf['port'], host='0.0.0.0')


def start(argv):
    print argv
    start = time.time()
    pa = argparse.ArgumentParser(description='Open Data Portal Watch API service.', prog='pwapi')

    logg = pa.add_argument_group("Logging")
    logg.add_argument(
        '-d', '--debug',
        help="Print lots of debugging statements",
        action="store_const", dest="loglevel", const=logging.DEBUG,
        default=logging.WARNING
    )

    logg.add_argument(
        '-v', '--verbose',
        help="Be verbose",
        action="store_const", dest="loglevel", const=logging.INFO,
        default=logging.WARNING
    )

    config = pa.add_argument_group("Config")
    config.add_argument('-c', '--config', help="config file", dest='config')
    args = pa.parse_args(args=argv)

    if args.config:
        try:
            with open(args.config) as f_conf:
                config = yaml.load(f_conf)
                if 'logging' in config:
                    print "setup logging"
                    logging.config.dictConfig(config['logging'])
                else:
                    ##load basic logging
                    logconf = os.path.join(odpw.__path__[0], 'resources/logging', 'logging.yaml')
                    with open(logconf) as f:
                        logging.config.dictConfig(yaml.load(f))

        except Exception as e:
            print "Exception during config initialisation", e
            return
    else:
        ##load basic logging
        logconf = os.path.join(odpw.__path__[0], 'resources/logging', 'logging.yaml')
        with open(logconf) as f:
            logging.config.dictConfig(yaml.load(f))
        logging.basicConfig(level=args.loglevel)

    # config the structlog
    config_logging()
    log = structlog.get_logger()

    #try:
    log.info("CMD ARGS", args=str(args))
    cli(args)
    #except Exception as e:
    #    log.fatal("Uncaught exception", exc_info=True)
    end = time.time()
    secs = end - start
    msecs = secs * 1000
    log.info("END MAIN", time_elapsed=msecs)

    Timer.printStats()
    ErrorHandler.printStats()


if __name__ == "__main__":
    start(sys.argv[1:])
