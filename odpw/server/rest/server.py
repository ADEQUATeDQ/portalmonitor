'''
Created on Nov 27, 2015

@author: jumbrich
'''
#!flask/bin/python
from flask import Flask

from odpw.db.dbm import PostgressDBM


from flask.globals import request
from flask.json import jsonify
from flask_swagger import swagger

from odpw.utils.timer import Timer
from odpw.db.models import PortalMetaData

import collections
import pandas as pd
from StringIO import StringIO
import urllib


#GZIPPED RESPONSE
from flask import after_this_request, request
from cStringIO import StringIO as IO
import gzip
import functools 
from flask.helpers import url_for, send_file, send_from_directory
from odpw.reporting.info_reports import portalinfo
from odpw.analysers import process_all
from odpw.analysers.core import DBAnalyser
from odpw.reporting.quality_reports import portalquality
from odpw.reporting.reporters import Report, SnapshotsPerPortalReporter
from flask.templating import render_template




from odpw.server.rest.api_blueprint import api


app = Flask(__name__)
from odpw.server.rest.cache import cache
app = Flask(__name__)
cache.init_app(app)




@app.route("/api/v1/help")
def helpDoc():
    return render_template("index.html", host=request.host)

@app.route('/api/v1/lib/<path:path>')
def send_js(path):
    print 'here'
    
    


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