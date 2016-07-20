import structlog
import yaml
from tornado.ioloop import IOLoop
from tornado.web import FallbackHandler, RequestHandler, Application
from tornado.wsgi import WSGIContainer

from odpw.new.core.db import DBManager, DBClient
from odpw.new.web.cache import cache
from odpw.new.web.rest.odpw_restapi_blueprint import restapi

log =structlog.get_logger()

from flask import Flask, jsonify, request

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

    dbc= DBClient(dbm)

    app.config['dbsession']=DbSession
    app.config['dbc']=dbc


    @app.teardown_appcontext
    def teardown(exception=None):
        if DbSession:
            DbSession.remove()

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


    app=create_app(dbm)
    app.register_blueprint(restapi,url_prefix=conf['url_prefix'])
    tr = WSGIContainer(app)

    application = Application([
        (r"/tornado", MainHandler),
        (r".*", FallbackHandler, dict(fallback=tr)),
    ])
    application.listen(conf['port'])
    print "Listinging on http://localhost:"+str(conf['port'])+"/"+conf['url_prefix']
    log.info("Server running", url="http://localhost:"+str(conf['port'])+"/"+conf['url_prefix'])
    IOLoop.instance().start()

if __name__ == "__main__":
    dbm=DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    app=create_app(dbm)
    app.register_blueprint(restapi,url_prefix='/api')

    tr = WSGIContainer(app)

    application = Application([
        (r"/tornado", MainHandler),
        (r".*", FallbackHandler, dict(fallback=tr)),
    ])
    print "here"
    application.listen(5122)
    print "Listinging on http://localhost:5122/api"
    IOLoop.instance().start()




#if __name__ == '__main__':



#    app.run( debug=True, port=5123)