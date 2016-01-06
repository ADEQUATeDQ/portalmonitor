'''
Created on Nov 27, 2015

@author: jumbrich
'''
#!flask/bin/python
from flask import Flask

from odpw.db.dbm import PostgressDBM
from odpw.server.rest.api_blueprint import api
from flask.globals import request
from flask.json import jsonify
from flask_swagger import swagger

app = Flask(__name__)
from odpw.server.rest.cache import cache
app = Flask(__name__)
cache.init_app(app)

#@app.errorhandler(500)
def internal_error(e, msg):
    app.logger.error(e)
    
    message = {
            'status': 500,
            'exception': e.message,
            'request_url': request.url
    }
    resp = jsonify(message)
    resp.status_code = 500

@app.route("/api/v1/spec")
def spec():
    swag = swagger(app)
    swag['info']['version'] = "1.0"
    swag['info']['title'] = "ODPW API"
    return jsonify(swag)

def name():
    return 'RestAPI'
def help():
    return "Start the REST API"

def setupCLI(pa):
    pa.add_argument('-p','--port',type=int, dest='port', default=2340)    

def cli(args,dbm):
    
    app.config['db']=dbm
    
    
    app.logger.info('Starting OPDW REST Service on http://localhost:{}/'.format(args.port))
    print('Starting OPDW REST Service on http://localhost:{}/'.format(args.port))
    
    app.register_blueprint(api,url_prefix='/api')
    app.run(debug=True, port = args.port)
    
    

if __name__ == '__main__':
    dbm = PostgressDBM(host='portalwatch.ai.wu.ac.at')
    
    app.config['db']=dbm
    #flasapp.register_blueprint(bp)
    #,ssl_context='adhoc'
    app.register_blueprint(api,url_prefix='/api')
    app.run(debug=True, port=5123)