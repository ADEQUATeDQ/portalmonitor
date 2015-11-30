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

import collections
import pandas as pd
from StringIO import StringIO
##### QA keys
qakeys=[
        'ExAc',
        'ExCo',
        'ExDa',
        'ExDi',
        'ExPr',
        'ExRi',
        'ExSp',
        'ExTe',
        'CoAc',
        'CoCE',
        'CoCU',
        'CoDa',
        'CoFo',
        'CoLi',
        'OpFo',
        'OpLi',
        'OpMa'
        ]

#GZIPPED RESPONSE
from flask import after_this_request, request
from cStringIO import StringIO as IO
import gzip
import functools 
from flask.helpers import url_for, send_file




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
from flask.ext.cache import Cache
cache = Cache(app,config={'CACHE_TYPE': 'simple'})

@app.errorhandler(404)
def not_found(error=None):
    message = {
            'status': 404,
            'message': 'Not Found: ' + request.url,
    }
    resp = jsonify(message)
    resp.status_code = 404

    return resp


@app.before_request
@cache.cached(timeout=300)  # cache this view for 5 minutes
def before_request():
    p={}
    dbm= app.config['db']
    print "get portals"
    for por in dbm.getPortals():
        p[ por['id'] ]= {   'iso3':por['iso3'],
                            'software':por['software'],
                            'url': por['url']
                        }
    
    app.config['portals']= p



@app.route('/api', methods=['GET'])
@cache.cached(timeout=300)  # cache this view for 5 minutes
def index():
    print app.config['db']
    return "Hello, World!"


@app.route('/api/v1/portals/list', methods=['GET'])
@cache.cached(timeout=300)  # cache this view for 5 minutes
def portalList():
    dbm= app.config['db']
    with Timer(key='portal/list', verbose=True) as t:
        try:
            data=[]
            p=app.config['portals']
            
            for pRes in dbm.getLatestPortalMetaDatas():
                rdict= dict(pRes)
                if rdict['portal_id'] in p:
                    rdict.update(p[rdict['portal_id']])
                    data.append(rdict)
                    
            #print "data"    
            #print data  
            resp = jsonify({'portals': data})
            resp.status_code = 200
            
            return resp
        except Exception as e:
            print e


@app.route('/api/v1/portals/quality/<int:snapshot>', methods=['GET'])
def quality(snapshot):
    dbm= app.config['db']
    with Timer(key='portalsQuality('+str(snapshot)+')', verbose=True) as t:
        
        filterSnapshot = request.args.get('since')
        filterIDs=set([])
        
        if filterSnapshot:
            for p in dbm.getPortalIDs(snapshot=filterSnapshot):
                filterIDs.add(p[0])
            
        p=app.config['portals']
        data={}
        cols=[]
        for pmd in PortalMetaData.iter(dbm.getPortalMetaDatas(snapshot)):
            if len(filterIDs)==0 or pmd.portal_id in filterIDs: 
                d=collections.OrderedDict()
                d['portal_id']=pmd.portal_id
                
                for k in qakeys:
                    v = pmd.qa_stats.get(k,-1)
                    if v is None:
                        v=-1
                    d['qa_'+k]=v
                
                
                d.update(p[pmd.portal_id])
                for k in ['snapshot', 'datasets', 'resources']:
                    d[k] = pmd.__dict__[k]
                
                
                cols=d.keys()
                data[pmd.portal_id]=(d)
                
        od = collections.OrderedDict(sorted(data.items()))
        pa = pd.DataFrame(od.values(), columns=cols)
        
        #for csv
        fname='odpw_quality_'+str(snapshot)
        if filterSnapshot:
            fname +='_since_'+str(filterSnapshot)
        
        buffer = StringIO()
        pa.to_csv(buffer,encoding='utf-8',index=False)
        buffer.seek(0)
        return send_file(buffer,
                         attachment_filename=fname+".csv",
                         mimetype='text/csv')
        
                
        #resp = jsonify({'data': data})
        #resp.status_code = 200
            
        #return resp

@app.route('/api/v1/help', methods = ['GET'])
def apihelp():
    """Print available functions."""
    func_list = {}
    for rule in app.url_map.iter_rules():
        if rule.endpoint != 'static':
            func_list[rule.rule] = app.view_functions[rule.endpoint].__doc__
    return jsonify(func_list)


def name():
    return 'RestAPI'
def help():
    return "Start the REST API"

def setupCLI(pa):
    pa.add_argument('-p','--port',type=int, dest='port', default=2340)    

def cli(args,dbm):
    
    app.config['db']=dbm
    
    #loading current portal list
    
    
    
    print('Starting OPDW REST Service on http://localhost:{}/'.format(args.port))
    app.run(debug=True, port = args.port)
    
    

if __name__ == '__main__':
    dbm = PostgressDBM(host='portalwatch.ai.wu.ac.at')
    
    app.config['db']=dbm
    #flasapp.register_blueprint(bp)
    #,ssl_context='adhoc'
    app.run(debug=True, port=5123)