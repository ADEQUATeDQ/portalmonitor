'''
Created on Jan 3, 2016

@author: jumbrich
'''

import logging
import os

import functools

from flask import after_this_request
from odpw.core.model import ResourceCrawlLog
from flask import request
from flask import send_file
from flask import url_for
from flask_restplus import Resource
from odpw.web_rest.rest.odpw_restapi import api
from odpw.utils.datamonitor_utils import parseDate

log = logging.getLogger(__name__)

ns = api.namespace('data', description='Operations related to blog categories')





from flask import Blueprint, current_app, render_template

from flask import jsonify,Response




# from cStringIO import StringIO as IO
# import gzip
# import functools
# from flask.ctx import after_this_request
# from flask.globals import request
# from flask.helpers import  url_for, send_file
# from datamonitor.datetime_utils import parseDate
# from werkzeug import redirect
# import os.path
# from datamonitor.timer import Timer
# import json
#
# api = Blueprint('api', __name__,
#                     template_folder='templates',
#                     static_folder='static',
#                     )

from datetime import datetime



# @api.errorhandler(404)
# def not_found(error=None):
#     message = {
#             'status': 404,
#             'message': 'Not Found: ' + request.url,
#     }
#     resp = jsonify(message)
#     resp.status_code = 404
#
#     return resp
#
# @api.route('/help1', methods = ['GET'])
# def help1():
#     """Print available functions."""
#     func_list = {}
#     for rule in current_app.url_map.iter_rules():
#         if rule.endpoint != 'static':
#            func_list[rule.rule] = current_app.view_functions[rule.endpoint].__doc__
#     return jsonify(func_list)

#===============================================================================
# @api.route('/data/<date>/<path:url>', methods=['GET'])
# def data(date, url):
#     with Timer(key='data2', verbose=True):
#         try:
#             
#             d = parseDate(date)
#             res = current_app.config['db'].getClosestVersion(url, d)
#         
#             pname = dict(res)['disklocation']
#             head, tail = os.path.split(pname)
#             head= head.replace("/data/datamonitor","")
#             
#             url= 'http://csvengine.ai.wu.ac.at/'+os.path.join(head,urllib.quote(tail))
#             print url
#             return redirect(url, code=302)
#             
#         except Exception as e:
#             raise e
#===============================================================================

def get_data(url, date):
    session = current_app.config['dbsession']
    res = session.query(ResourceCrawlLog.disklocation).filter(ResourceCrawlLog.uri == url).filter(
        ResourceCrawlLog.timestamp > date).order_by(ResourceCrawlLog.timestamp - date).first()
    pname = res[0]
    head, tail = os.path.split(pname)
    head = head.replace("/data/datamonitor", "")

    filename = os.path.join(head, tail)
    if not os.path.isfile(filename):
        current_app.logger.error("File %s does not exists", filename)

    return send_file(filename, as_attachment=True)


@ns.route('/data/<path:url>')
@ns.route('/memento/<path:url>')
@ns.doc(params={'url': 'URL (HTTP URL)'})
class GetDataWithoutDate(Resource):

    def get(self, url):
        d = datetime.now()
        return get_data(url, d)


@ns.route('/data/<date>/<path:url>')
@ns.route('/memento/<date>/<path:url>')
@ns.doc(params={'url': 'URL (HTTP URL)', 'date':'Date as \"YYYY<MM|DD|HH|MM|SS>\"'})
class GetData(Resource):
    def get(self, date, url):
        d = parseDate(date)
        return get_data(url, d)

@ns.route('/list')
class List(Resource):

    def get(self):
        session = current_app.config['dbsession']
        q=session.query(ResourceCrawlLog.uri).distinct(ResourceCrawlLog.uri)
        urls=[]
        for r in q:
           urls.append(r[0])

        resp = jsonify(urls)
        return resp

@ns.route('/timemap/json/<path:url>')
@ns.doc(params={'url': 'URL (HTTP URL)', 'date': 'Date as \"YYYY<MM|DD|HH|MM|SS>\"'})
class Timemap(Resource):
    def get(self, url):
        session = current_app.config['dbsession']
        res = {
                "original_uri": url
                ,"timemap_uri": {
                                "json_format": url_for('api.data_timemap', url=url),
                                #"link_format": "http://labs.mementoweb.org/timemap/link/http://cnn.com/"
                                }
                , "mementos": {'list':[]}
                }

        d={}
        q=session.query(ResourceCrawlLog.timestamp).filter(ResourceCrawlLog.uri == url).filter(ResourceCrawlLog.contentchanged!=0).order_by(ResourceCrawlLog.timestamp)
        for r in q:

            dt = r[0]

            dt1=dt.replace(microsecond=0)
            uri=url_for('api.data_get_data', date=dt1.strftime("%Y%m%d%H%M%S"), url=url, _external=True)
            d={'datetime':dt1, 'uri':uri}

            if 'first' not in res['mementos']:
                res['mementos']['first']=d
            res['mementos']['list'].append(d)
        res['mementos']['last']=d

        resp = jsonify(res)
        return resp
