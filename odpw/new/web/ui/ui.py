from jinja2 import Environment
from sqlalchemy.orm import scoped_session, sessionmaker
from tornado.ioloop import IOLoop
from tornado.web import FallbackHandler, RequestHandler, Application, urlparse
from tornado.wsgi import WSGIContainer

from odpw.new.model import Base
from odpw.new.web.ui.odpw_ui_blueprint import ui
from odpw.new.db import DBManager

from odpw.new.web.cache import cache


from flask import Flask, jsonify, request




# database session registry object, configured from
# create_app factory

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
    Base.query = DbSession.query_property()
    app.config['dbsession']=DbSession

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

    app=create_app(dbm)
    app.register_blueprint(ui,url_prefix='/api')

    tr = WSGIContainer(app)

    application = Application([
        (r"/tornado", MainHandler),
        (r".*", FallbackHandler, dict(fallback=tr)),
    ])

    application.listen(5123)
    IOLoop.instance().start()

if __name__ == "__main__":
    dbm=DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    app=create_app(dbm)
    app.register_blueprint(ui,url_prefix='/ui')

    tr = WSGIContainer(app)

    application = Application([
        (r"/tornado", MainHandler),
        (r".*", FallbackHandler, dict(fallback=tr)),
    ])
    print "here"
    application.listen(5124)
    IOLoop.instance().start()




#if __name__ == '__main__':



#    app.run( debug=True, port=5123)