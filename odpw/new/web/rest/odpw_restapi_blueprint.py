# -*- coding: utf-8 -*-
import StringIO
import csv
import json
from datetime import timedelta
from functools import update_wrapper
from functools import wraps

from flask import Blueprint, jsonify, render_template, Response
from flask import make_response, request, current_app
from sqlalchemy import and_

from odpw.new.core.db import row2dict
from odpw.new.core.model import Portal, PortalSnapshotQuality, PortalSnapshot, ResourceInfo, Base
from odpw.new.web.cache import cache


def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator


restapi = Blueprint('api', __name__,
                    template_folder='../templates',
                    static_folder='../static',
                    )


def toCSV(func):
    @wraps(func)
    def decorated_function(*args, **kwargs):
        results= func(*args, **kwargs)

        keys = results[0].keys()

        si = StringIO.StringIO()
        cw = csv.DictWriter(si,keys)
        cw.writeheader()
        cw.writerows(results)

        output = make_response(si.getvalue())
        #output.headers["Content-Disposition"] = "attachment; filename=portals.csv"
        output.headers["Content-type"] = "text/csv"
        return output

    return decorated_function

def toJSON(func):
    @wraps(func)
    def decorated_function(*args, **kwargs):
        results= func(*args, **kwargs)
        return Response(json.dumps(results),  mimetype='application/json')

    return decorated_function



@restapi.route('/help', methods=['GET'])
def help():
    return render_template('apidoc.jinja')


@restapi.route('/spec', methods=['GET'])
def spec():
    return render_template('spec.json', host="localhost:5122/", basePath="api")

@restapi.route('/', methods=['GET'])
def index():
    return render_template('api.html')


@restapi.route('/help1', methods = ['GET'])
def help1():
    """Print available functions."""
    func_list = {}
    for rule in current_app.url_map.iter_rules():
        if rule.endpoint != 'static':
           func_list[rule.rule] = current_app.view_functions[rule.endpoint].__doc__
    return jsonify(func_list)


#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
######## SYSTEM ######

#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#




#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
######## PORTALS ######
### PORTALS
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
@cache.cached(timeout=300)
def _portals():
    dbc=current_app.config['dbc']
    return [row2dict(i) for i in dbc.portals()]

@restapi.route('/portals', methods = ['GET'])
@toJSON
def portals():
    return _portals()

@restapi.route('/portals.csv', methods = ['GET'])
@toCSV
def portalsCSV():
    return _portals()

#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
### PORTALS FETCH
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
@cache.cached(timeout=300)
def _portalsfetch(snapshot):

    dbc=current_app.config['dbc']
    return [ row2dict(i) for i in dbc.portalsSnapshots(snapshot=snapshot) ]

@restapi.route('/portals/<int:snapshot>/fetch', methods = ['GET'])
@toJSON
def portalsfetch(snapshot):

    return _portalsfetch(snapshot)

@restapi.route('/portals/<int:snapshot>/fetch.csv', methods = ['GET'])
@toCSV
def portalsfetchCSV(snapshot):

    return _portalsfetch(snapshot)

#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
### PORTALS QUALITY
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
@cache.cached(timeout=300)
def _portalsquality(snapshot):

    dbc=current_app.config['dbc']
    return [ row2dict(i) for i in dbc.portalsQuality(snapshot=snapshot) ]


@restapi.route('/portals/<int:snapshot>/quality', methods = ['GET'])
@toJSON
def portalsquality(snapshot):
    return _portalsquality(snapshot)

@restapi.route('/portals/<int:snapshot>/quality.csv', methods = ['GET'])
@toCSV
def portalsqualityCSV(snapshot):
    return _portalsquality(snapshot)

#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
### PORTALS ALL INFO
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
@cache.cached(timeout=300)
def _portalsall(snapshot):

    dbc=current_app.config['dbc']
    return [row2dict(i) for i in dbc.portalsAll(snapshot=snapshot) ]

@restapi.route('/portals/<int:snapshot>/all', methods = ['GET'])
@toJSON
def portalsall(snapshot):
    return _portalsall(snapshot)

@restapi.route('/portals/<int:snapshot>/all.csv', methods = ['GET'])
@toCSV
def portalsallCSV(snapshot):
    return _portalsall(snapshot)

#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
######## PORTAL ######
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#

#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
## PORTAL SNAPSHOT ALL
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
@cache.cached(timeout=300)
def _portalAll(portalid, snapshot=None):
    q=PortalSnapshot.query
    if snapshot is not None:
        q=q.filter(PortalSnapshotQuality.snapshot==snapshot)
    q=q.filter(PortalSnapshot.portalid==portalid)\
        .outerjoin(PortalSnapshotQuality, and_(PortalSnapshot.portalid==PortalSnapshotQuality.portalid,PortalSnapshot.snapshot==PortalSnapshotQuality.snapshot))\
        .join(Portal)\
        .add_entity(PortalSnapshotQuality)\
        .add_entity(Portal)

    print 'Query',str(q)
    return [row2dict(i) for i in q.all()]

@restapi.route('/portal/<portalid>/<int:snapshot>/all', methods = ['GET'])
@toJSON
def portalSnapshotAll(snapshot,portalid):
    return _portalAll(portalid, snapshot)

@restapi.route('/portal/<portalid>/<int:snapshot>/all.csv', methods = ['GET'])
@toCSV
def portalSnapshotAllCSV(snapshot, portalid):
    return _portalAll(portalid, snapshot)

#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
## PORTAL ALL
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
@restapi.route('/portal/<portalid>/all', methods = ['GET'])
@toJSON
def portalAll(portalid):
    return _portalAll(portalid)

#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
## PORTAL SNAPSHOT QUALITY
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
@cache.cached(timeout=300)
def _portalQuality(portalid, snapshot=None):
    q=PortalSnapshotQuality.query
    if snapshot:
        q=q.filter(PortalSnapshotQuality.snapshot==snapshot)
    q= q.filter(PortalSnapshotQuality.portalid==portalid)\
        .all()
    return [row2dict(i) for i in q]

@restapi.route('/portal/<portalid>/<int:snapshot>/quality', methods = ['GET'])
@toJSON
def portalSnapshotQuality(snapshot,portalid):
    return _portalQuality(portalid, snapshot)

@restapi.route('/portal/<portalid>/<int:snapshot>/quality.csv', methods = ['GET'])
@toCSV
def portalSnapshotQualityCSV(snapshot, portalid):
    return _portalQuality(portalid, snapshot)

#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
## PORTAL QUALITY
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
@restapi.route('/portal/<portalid>/quality', methods = ['GET'])
@toJSON
def portalQuality(portalid):
    return _portalQuality(portalid)


#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
## PORTAL SNAPSHOT RESOURCES
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#

@cache.cached(timeout=300)
def _portalResources(portalid, snapshot):
    dbc=current_app.config['dbc']
    return  [row2dict(i) for i in dbc.getResourceInfos(snapshot,portalid=portalid) ]

@restapi.route('/portal/<portalid>/<int:snapshot>/resources.csv', methods = ['GET'])
@toCSV
def portalResourcesCSV(portalid,snapshot):
    return _portalResources(portalid,snapshot)


@restapi.route('/portal/<portalid>/<int:snapshot>/resources', methods = ['GET'])
@crossdomain(origin='*',headers=['Content- Type','Authorization'])
def portalResources(portalid,snapshot):
    results={}
    dbc=current_app.config['dbc']
    for i in dbc.getResourceInfos(snapshot,portalid=portalid):
        results= _portalResources(portalid,snapshot)



    print results
    return Response(json.dumps(results),  mimetype='application/json')

