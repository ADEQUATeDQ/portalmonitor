import structlog
log =structlog.get_logger()

import yaml
from sqlalchemy.orm import scoped_session, sessionmaker
from tornado.ioloop import IOLoop
from tornado.web import FallbackHandler, RequestHandler, Application
from tornado.wsgi import WSGIContainer

from odpw.new.web_rest.cache import cache
from odpw.new.web_rest.ui.odpw_ui_blueprint import ui

from odpw.new.core.api import DBClient
from flask import Flask, jsonify, request

class MainHandler(RequestHandler):
    def get(self):
      self.write("This message comes from Tornado ^_^")

def create_app(dbm):
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
        'url_prefix':'api'
        ,'port':5123
    }
    if args.config:
        with open(args.config) as f_conf:
            config = yaml.load(f_conf)
            if 'ui' in config:
                for key in conf:
                    if key in config['ui']:
                        conf[key]=config['ui'][key]

    app=create_app(dbm)
    app.register_blueprint(ui,url_prefix=conf['url_prefix'])
    tr = WSGIContainer(app)

    application = Application([
        (r"/tornado", MainHandler),
        (r".*", FallbackHandler, dict(fallback=tr)),
    ])
    application.listen(conf['port'])
    print "Listinging on http://localhost:"+str(conf['port'])+"/"+conf['url_prefix']
    log.info("Server running", url="http://localhost:"+str(conf['port'])+"/"+conf['url_prefix'])
    IOLoop.instance().start()
