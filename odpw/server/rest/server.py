'''
Created on Nov 27, 2015

@author: jumbrich
'''
#!flask/bin/python
from flask import Flask, render_template
from flask import request
from flask import Response
from flask import jsonify

from odpw.db.dbm import PostgressDBM
from odpw.utils.timer import Timer
from odpw.db.models import PortalMetaData


#CACHING
from werkzeug.contrib.cache import SimpleCache
CACHE_TIMEOUT = 300

cache = SimpleCache()
class cached(object):

    def __init__(self, timeout=None):
        self.timeout = timeout or CACHE_TIMEOUT

    def __call__(self, f):
        def decorator(*args, **kwargs):
            response = cache.get(request.path)
            if response is None:
                print "caching ", request.path
                response = f(*args, **kwargs)
                cache.set(request.path, response, self.timeout)
            return response
        return decorator
    
#GZIPPED RESPONSE
from flask import after_this_request, request
from cStringIO import StringIO as IO
import gzip
import functools 

def gzipped(f):
    @functools.wraps(f)
    def view_func(*args, **kwargs):
        @after_this_request
        def zipper(response):
            accept_encoding = request.headers.get('Accept-Encoding', '')

            if 'gzip' not in accept_encoding.lower():
                return response

            response.direct_passthrough = False

            if (response.status_code < 200 or
                response.status_code >= 300 or
                'Content-Encoding' in response.headers):
                return response
            gzip_buffer = IO()
            gzip_file = gzip.GzipFile(mode='wb', 
                                      fileobj=gzip_buffer)
            gzip_file.write(response.data)
            gzip_file.close()

            response.data = gzip_buffer.getvalue()
            response.headers['Content-Encoding'] = 'gzip'
            response.headers['Vary'] = 'Accept-Encoding'
            response.headers['Content-Length'] = len(response.data)

            return response

        return f(*args, **kwargs)

    return view_func    

app = Flask(__name__)

@app.errorhandler(404)
def not_found(error=None):
    message = {
            'status': 404,
            'message': 'Not Found: ' + request.url,
    }
    resp = jsonify(message)
    resp.status_code = 404

    return resp



@app.route('/api', methods=['GET'])
def index():
    print app.config['db']
    return "Hello, World!"


@app.route('/api/v1/portals/list', methods=['GET'])
@cached()
def list():
    dbm= app.config['db']
    with Timer(key='portal/list', verbose=True) as t:
        try:
            data=[]
            p={}
            for por in dbm.getPortals():
                p[ por['id'] ]= {   'iso3':por['iso3'],
                                    'software':por['software'],
                                    'url': por['url']
                                }
            for pRes in dbm.getLatestPortalMetaDatas():
                rdict= dict(pRes)
                if rdict['portal_id'] in p:
                    rdict.update(p[rdict['portal_id']])
                    data.append(rdict)
                    
            print "data"    
            print data  
            resp = jsonify({'portals': data})
            resp.status_code = 200
            
            return resp
        except Exception as e:
            print e


@app.route('/api/v1/portals/quality/<int:snapshot>')
@gzipped
def quality(snapshot):
    dbm= app.config['db']
    with Timer(key='portalsQuality('+str(snapshot)+')', verbose=True) as t:
        
        for pmd in PortalMetaData.iter(dbm.getPortalMetaDatas(snapshot)):
            print pmd.qa_stats
            
        resp = jsonify({'portals': {}})
        resp.status_code = 200
            
        return resp

def name():
    return 'RestAPI'
def help():
    return "Start the REST API"

def setupCLI(pa):
    pa.add_argument('-p','--port',type=int, dest='port', default=2340)    

def cli(args,dbm):
    
    app.config['db']=dbm
    
    print('Starting OPDW REST Service on http://localhost:{}/'.format(args.port))
    app.run(debug=True, port = args.port)
    
    

if __name__ == '__main__':
    dbm = PostgressDBM(host='portalwatch.ai.wu.ac.at')
    
    app.config['db']=dbm
    #flasapp.register_blueprint(bp)
    #,ssl_context='adhoc'
    app.run(debug=True, port=5123)