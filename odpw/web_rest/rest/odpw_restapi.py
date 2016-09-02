# -*- coding: utf-8 -*-


import logging
import traceback
from flask import Blueprint, jsonify, render_template, Response
from flask import current_app

from odpw.web_rest.rest import settings
from sqlalchemy.orm.exc import NoResultFound

log = logging.getLogger(__name__)


from flask_restplus import Api
api = Api(version='1.0', title='ODPW Rest API',
          description='ODPW')

@api.errorhandler
def default_error_handler(error):
    '''Default error handler'''
    return {'message': str(error)}, getattr(error, 'code', 500)


@api.errorhandler(NoResultFound)
def database_not_found_error_handler(e):
    log.warning(traceback.format_exc())
    return {'message': 'A database result was required but none was found.'}, 404


#
# @restapi.route('/help', methods=['GET'])
# def help():
#     return render_template('apidoc.jinja')
#
#
# @restapi.route('/spec', methods=['GET'])
# def spec():
#     return render_template('spec.json', host="localhost:5122/", basePath="api")
#
# @restapi.route('/', methods=['GET'])
# def index():
#     return render_template('api.html')
#
#
# @restapi.route('/help1', methods = ['GET'])
# def help1():
#     """Print available functions."""
#     func_list = {}
#     for rule in current_app.url_map.iter_rules():
#         if rule.endpoint != 'static':
#            func_list[rule.rule] = current_app.view_functions[rule.endpoint].__doc__
#     return jsonify(func_list)
