# -*- coding: utf-8 -*-
import json
import logging

from flask import current_app, Response

from odpw.new.web_rest.rest.odpw_restapi import api
from odpw.new.web_rest.rest.helpers import toJSON, toCSV
from odpw.new.core.db import row2dict
from odpw.new.core.model import Portal, PortalSnapshotQuality, PortalSnapshot, ResourceInfo, Base
from odpw.new.web_rest.cache import cache


from flask_restplus import Resource

log = logging.getLogger(__name__)

ns = api.namespace('portals', description='Operations related to the set of portals in the system')
from flask import request
from flask_restplus import Resource

from flask_restplus import reqparse

pagination_arguments = reqparse.RequestParser()
pagination_arguments.add_argument('page', type=int, required=False, default=1, help='Page number')
pagination_arguments.add_argument('bool', type=bool, required=False, default=1, help='Page number')
pagination_arguments.add_argument('per_page', type=int, required=False, choices=[2, 10, 20, 30, 40, 50],
                                  default=10, help='Results per page {error_msg}')


from flask_restplus import fields
portal = ns.model('Portal', {
    "uri": fields.String(required=True, description='Homepage of portal'),
    "active": fields.Boolean(required=True, description='I the portal still active'),
    "iso": fields.String(required=True, description='ISO2 country code'),
    "apiuri": fields.String(required=True, description='API URI of the portal'),
    "id": fields.String(required=True, description='Internal Portal id'),
    "software": fields.String(required=True, description='Software powering the portal'),

})

portalstats = ns.inherit('PortalStats',  portal, {
    "snapshot_count": fields.Integer(required=True, description='Software powering the portal'),
    "last_snapshot": fields.Integer(required=True, description='Software powering the portal'),
    "datasetCount": fields.Integer(required=True, description='Software powering the portal'),
    "resourceCount": fields.Integer(required=True, description='Software powering the portal'),
})



@ns.route('/list')
class Portals(Resource):

    #@ns.expect(pagination_arguments)
    @api.marshal_with(portal, as_list=True)

    @ns.doc('get all portal information for snapshot')
    def get(self):
        """
        Returns list of portals.
        """
        #args = pagination_arguments.parse_args(request)
        #page = args.get('page', 1)
        #per_page = args.get('per_page', 10)
        session=current_app.config['dbsession']
        data= [row2dict(i) for i in session.query(Portal).all()]
        return data

@ns.route('/list_with_stats')
class PortalsStats(Resource):

    #@ns.expect(pagination_arguments)
    @api.marshal_with(portalstats, as_list=True)

    @ns.doc('get all portal information for snapshot')
    def get(self):
        """
        Returns list of portals with additon stats.
        """
        #args = pagination_arguments.parse_args(request)
        #page = args.get('page', 1)
        #per_page = args.get('per_page', 10)
        session=current_app.config['dbsession']
        data=[row2dict(r) for r in session.query(Portal, Portal.snapshot_count, Portal.first_snapshot, Portal.last_snapshot, Portal.datasetCount, Portal.resourceCount)]

        return data

@ns.route('/quality/<int:snapshot>')
class PortalsQuality(Resource):

    #@ns.expect(pagination_arguments)
    #@api.marshal_with(portalsquality, as_list=True)

    @ns.doc('get_PortalsQuality')
    def get(self, snapshot):
        """
       get list of portals with their quality assessment metrics for the specified snapshot
        """
        #args = pagination_arguments.parse_args(request)
        #page = args.get('page', 1)
        #per_page = args.get('per_page', 10)
        session=current_app.config['dbsession']
        data=[row2dict(r) for r in session.query(Portal, Portal.datasetCount, Portal.resourceCount).join(PortalSnapshotQuality).filter(PortalSnapshotQuality.snapshot==snapshot).add_entity(PortalSnapshotQuality)]

        return data

@ns.route('/quality')
class PortalsCurQuality(Resource):

    #@ns.expect(pagination_arguments)
    #@api.marshal_with(portalsquality, as_list=True)

    @ns.doc('get_PortalsCurQuality')
    def get(self):
        """
        get list of portals with their current quality assessment metrics.
        """
        #args = pagination_arguments.parse_args(request)
        #page = args.get('page', 1)
        #per_page = args.get('per_page', 10)
        session=current_app.config['dbsession']
        #snapshot=getPreviousWeek(getSnapshotfromTime(datetime.datetime.now()))
        data=[row2dict(r) for r in session.query(Portal, Portal.datasetCount, Portal.resourceCount).join(PortalSnapshotQuality).filter(PortalSnapshotQuality.snapshot==snapshot).add_entity(PortalSnapshotQuality)]

        return data




#
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# ######## PORTALS ######
# ### PORTALS
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# @cache.cached(timeout=300)
# def _portals():
#     dbc=current_app.config['dbc']
#     return [row2dict(i) for i in dbc.portals()]
#
# @restapi.route('/portals', methods = ['GET'])
# @toJSON
# def portals():
#     return _portals()
#
# @restapi.route('/portals.csv', methods = ['GET'])
# @toCSV
# def portalsCSV():
#     return _portals()
#
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# ### PORTALS FETCH
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# @cache.cached(timeout=300)
# def _portalsfetch(snapshot):
#
#     dbc=current_app.config['dbc']
#     return [ row2dict(i) for i in dbc.portalsSnapshots(snapshot=snapshot) ]
#
# @restapi.route('/portals/<int:snapshot>/fetch', methods = ['GET'])
# @toJSON
# def portalsfetch(snapshot):
#
#     return _portalsfetch(snapshot)
#
# @restapi.route('/portals/<int:snapshot>/fetch.csv', methods = ['GET'])
# @toCSV
# def portalsfetchCSV(snapshot):
#
#     return _portalsfetch(snapshot)
#
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# ### PORTALS QUALITY
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# @cache.cached(timeout=300)
# def _portalsquality(snapshot):
#
#     dbc=current_app.config['dbc']
#     return [ row2dict(i) for i in dbc.portalsQuality(snapshot=snapshot) ]
#
#
# @restapi.route('/portals/<int:snapshot>/quality', methods = ['GET'])
# @toJSON
# def portalsquality(snapshot):
#     return _portalsquality(snapshot)
#
# @restapi.route('/portals/<int:snapshot>/quality.csv', methods = ['GET'])
# @toCSV
# def portalsqualityCSV(snapshot):
#     return _portalsquality(snapshot)
#
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# ### PORTALS ALL INFO
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# @cache.cached(timeout=300)
# def _portalsall(snapshot):
#
#     dbc=current_app.config['dbc']
#     return [row2dict(i) for i in dbc.portalsAll(snapshot=snapshot) ]
#
# @restapi.route('/portals/<int:snapshot>/all', methods = ['GET'])
# @toJSON
# def portalsall(snapshot):
#     return _portalsall(snapshot)
#
# @restapi.route('/portals/<int:snapshot>/all.csv', methods = ['GET'])
# @toCSV
# def portalsallCSV(snapshot):
#     return _portalsall(snapshot)
