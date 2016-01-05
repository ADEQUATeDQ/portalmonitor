'''
Created on Nov 27, 2015

@author: jumbrich
'''
#!flask/bin/python
from flask import Flask, render_template
from flask import request
from flask import Response
from flask import jsonify
from flask_swagger import swagger

from odpw.db.dbm import PostgressDBM
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

@app.errorhandler(500)
def internal_error(e, msg):
    app.logger.error(e)
    
    message = {
            'status': 500,
            'exception': e.message,
            'request_url': request.url
    }
    resp = jsonify(message)
    resp.status_code = 500

    
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

@app.route('/api/v1/portal/<string:portal_id>/quality/<int:snapshot>', methods=['GET'])
@cache.cached(timeout=300)  # cache this view for 5 minutes
def portalQuality(portal_id, snapshot):
    """
        Get quality metrics for a portal and snapshot 
        ---
        tags:
          - portal
        parameters:
          - in: path
            name: snapshot
            type: integer
            description: Snapshot as integer (YYWW, e.g. 1542 -> year 2015 week 42)
            required: true
          - in: path
            name: portal_id
            type: string
            required: true
        produces:
          - application/json
        responses:
          200:
            description: Returns a list of all portals in the system
        """
    dbm= app.config['db']
    with Timer(key='portal/'+portal_id+'/quality/'+str(snapshot), verbose=True) as t:
        try:
            a= process_all( DBAnalyser(), dbm.getSnapshots( portalID=portal_id,apiurl=None))
            rep=SnapshotsPerPortalReporter(a, portal_id)

            r = portalquality(dbm, snapshot, portal_id)
            rep = Report([rep,r])
            
            
            print rep.uireport()
            results = {'portal':portal_id,'snapshot':snapshot, 'results': rep.uireport()}
    
             
            pmd = dbm.getPortalMetaData(portalID=portal_id, snapshot=snapshot)
            d=collections.OrderedDict()
            d['portal_id']=pmd.portal_id
            for k in qakeys:
                
                v = pmd.qa_stats.get(k,-1) if pmd.qa_stats else None
                if v is None:
                    v=-1
                d['qa_'+k]=v
                
                #for k in ['snapshot', 'datasets', 'resources']:
                #    d[k] = pmd.__dict__[k]
                
            results['quality']=d

            # quality historgrams
            hists = collections.OrderedDict()
            for k in qakeys:
                v = pmd.qa_stats.get(k + '_hist', None) if pmd.qa_stats else None
                if v:
                    hists['qa_'+k] = v
            results['quality_counts'] = hists

            print results
            resp = jsonify(results)
            resp.status_code = 200
            
            return resp
        except Exception as e:
            print e
            internal_error(e,'')

@app.route('/api/v1/portal/<string:portal_id>/info/<int:snapshot>', methods=['GET'])
@cache.cached(timeout=300)  # cache this view for 5 minutes
def portalInfo(portal_id, snapshot):
    """
        Get basic information about a portal for a snapshot
        ---
        tags:
          - portal
        parameters:
          - in: path
            name: snapshot
            type: integer
            description: Snapshot as integer (YYWW, e.g. 1542 -> year 2015 week 42)
            required: true
          - in: path
            name: portal_id
            type: string
            required: true
        produces:
          - application/json
        responses:
          200:
            description: Returns a list of all portals in the system
        """
    dbm= app.config['db']
    with Timer(key='portal/'+portal_id+'/info/'+str(snapshot), verbose=True) as t:
        try:
            r = portalinfo(dbm, snapshot, portal_id)
        
            results = {'portal':portal_id,'snapshot':snapshot, 'results': r.uireport()}
        
            print results
            resp = jsonify(results)
            resp.status_code = 200
            
            return resp
        except Exception as e:
            internal_error(e,'')

@app.route('/api/v1/portals/list', methods=['GET'])
@cache.cached(timeout=300)  # cache this view for 5 minutes
def portalList():
    """
        Get a list of all portals
        ---
        tags:
          - portals
        produces:
          - application/json
        responses:
          200:
            description: Returns a list of all portals in the system
            schema:
              id: portals
              properties:
                portals:
                  type: array
                  items:
                    schema:
                      id: SubItem
                      properties:
                        datasets:
                          type: integer
                          description: Number of datasets
                        resources:
                          type: integer
                          description: Number of resources
        """
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
            internal_error(e,'')


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
        'OpMa',
        'ReDa',
        'ReRe',
        'AcFo',
        'AcSi'
        ]

@app.route('/api/v1/portals/quality/<int:snapshot>', methods=['GET'])
@gzipped
def quality(snapshot):
    """
        Get a all portals quality information for a certain snapshot
        ---
        tags:
          - portals
        produces:
          - text/csv
        parameters:
          - in: path
            name: snapshot
            type: integer
            description: Snapshot as integer (YYWW, e.g. 1542 -> year 2015 week 42)
            required: true
          - in: query
            name: since
            type: integer
        responses:
          200:
            description: Returns a list of basic information and quality metrics for all portals in the system
            schema:
              id: portals
              properties:
                portals:
                  type: array
                  items:
                    schema:
                      id: SubItem
                      properties:
                        datasets:
                          type: integer
                          description: Number of datasets
                        resources:
                          type: integer
                          description: Number of resources
        """
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
                    
                    v = pmd.qa_stats.get(k,-1) if pmd.qa_stats else None
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


@app.route('/api/v1/portals/ckanquality/<int:snapshot>', methods=['GET'])
@gzipped
def ckanquality(snapshot):
    """
        Get a all CKAN portals quality metrics for a certain snapshot
        ---
        tags:
          - portals
        produces:
          - text/csv
        parameters:
          - in: path
            name: snapshot
            type: integer
            description: Snapshot as integer (YYWW, e.g. 1542 -> year 2015 week 42)
            required: true
          - in: query
            name: since
            type: integer
        responses:
          200:
            description: Returns a list of basic information and CKAN specific quality metrics for all CKAN portals in the system
            schema:
              id: portals
              properties:
                portals:
                  type: array
                  items:
                    schema:
                      id: SubItem
                      properties:
                        datasets:
                          type: integer
                          description: Number of datasets
                        resources:
                          type: integer
                          description: Number of resources
        """
    dbm= app.config['db']
    with Timer(key='CkanPortalsQuality('+str(snapshot)+')', verbose=True) as t:

        filterSnapshot = request.args.get('since')
        filterIDs=set([])

        if filterSnapshot:
            for p in dbm.getPortalIDs(snapshot=filterSnapshot):
                filterIDs.add(p[0])

        p=app.config['portals']
        data={}
        cols=[]
        for pmd in PortalMetaData.iter(dbm.getPortalMetaDatasBySoftware(snapshot=snapshot, software='CKAN')):
            if len(filterIDs)==0 or pmd.portal_id in filterIDs:
                d=collections.OrderedDict()
                d['portal_id']=pmd.portal_id

                # get all ckan metrics
                ms = ['Qu', 'Qc']
                for m in ms:
                    cu = pmd.qa_stats.get(m, None) if pmd.qa_stats else None
                    for k in ['core', 'extra', 'res']:
                        if cu and k in cu:
                            d[m+'_'+k] = cu[k]
                        else:
                            d[m+'_'+k] = -1

                # openness
                qo = pmd.qa_stats.get('Qo', None) if pmd.qa_stats else None
                qo_mapping = {'format': 'f', 'license': 'l'}
                for k in qo_mapping:
                    if qo and k in qo:
                        v = qo[k]
                    else:
                        v = -1
                    d['Qo_'+qo_mapping[k]] = v

                # contactability
                qa = pmd.qa_stats.get('Qa', None) if pmd.qa_stats else None
                for k in ['url', 'email']:
                    if qa and k in qa:
                        v = qa[k]['total']
                    else:
                        v = -1
                    d['Qa_'+k] = v

                # qr_ds, qr_res
                try:
                    qr_ds = pmd.qa_stats['DatasetRetrievability']['DatasetRetrievability']['avgP']['qrd']
                except:
                    qr_ds = -1
                d['Qr_ds'] = qr_ds

                try:
                    qr_res = pmd.qa_stats['ResourceRetrievability']['ResourceRetrievability']['avgP']['qrd']
                except:
                    qr_res = -1
                d['Qr_res'] = qr_res

                d.update(p[pmd.portal_id])
                for k in ['snapshot', 'datasets', 'resources']:
                    d[k] = pmd.__dict__[k]

                cols=d.keys()
                data[pmd.portal_id]=(d)

        od = collections.OrderedDict(sorted(data.items()))
        pa = pd.DataFrame(od.values(), columns=cols)

        #for csv
        fname='ckan_quality_'+str(snapshot)
        if filterSnapshot:
            fname +='_since_'+str(filterSnapshot)

        buffer = StringIO()
        pa.to_csv(buffer,encoding='utf-8',index=False)
        buffer.seek(0)
        return send_file(buffer,
                         attachment_filename=fname+".csv",
                         mimetype='text/csv')


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
    app.run(debug=True, port = args.port)
    
    

if __name__ == '__main__':
    dbm = PostgressDBM(host='portalwatch.ai.wu.ac.at')
    
    app.config['db']=dbm
    #flasapp.register_blueprint(bp)
    #,ssl_context='adhoc'
    app.run(debug=True, port=5123)