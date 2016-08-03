# -*- coding: utf-8 -*-
import json


from flask import make_response, request, current_app
from sqlalchemy import and_

from odpw.new.web_rest.rest.odpw_restapi import api
from odpw.new.core.db import row2dict
from odpw.new.core.model import Portal, PortalSnapshotQuality, PortalSnapshot


import logging

from flask import request
from flask_restplus import Resource

log = logging.getLogger(__name__)

ns = api.namespace('portal', description='Operations related to blog categories')




@ns.route('/<portalid>/<int:snapshot>/all')
@ns.doc(params={'portalid': 'A portal id', 'snapshot':'Snapshot in yyww format (e.g. 1639 -> 2016 week 30)'})
class PortalAll(Resource):

    def get(self, portalid,snapshot):
        q=PortalSnapshot.query
        if snapshot is not None:
            q=q.filter(PortalSnapshotQuality.snapshot==snapshot)
        q=q.filter(PortalSnapshot.portalid==portalid)\
            .outerjoin(PortalSnapshotQuality, and_(PortalSnapshot.portalid==PortalSnapshotQuality.portalid,PortalSnapshot.snapshot==PortalSnapshotQuality.snapshot))\
            .join(Portal)\
            .add_entity(PortalSnapshotQuality)\
            .add_entity(Portal)
        return [row2dict(i) for i in q.all()]


@ns.route('/<portalid>/<int:snapshot>/quality')
@ns.doc(params={'portalid': 'A portal id', 'snapshot':'Snapshot in yyww format (e.g. 1639 -> 2016 week 30)'})
class PortalSnapshotQuality1(Resource):

    def get(self, portalid,snapshot):
        print portalid, snapshot
        #with Timer(key="portalQuality",verbose=True):
        session=current_app.config['dbsession']

        q=session.query(PortalSnapshotQuality).filter(PortalSnapshotQuality.portalid==portalid).filter(PortalSnapshotQuality.snapshot==snapshot)
        data=[row2dict(r) for r in q.all()]

        return data



#
#
#
#
#
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# ######## PORTAL ######
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
#
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# ## PORTAL SNAPSHOT ALL
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# @cache.cached(timeout=300)
# def _portalAll(portalid, snapshot=None):
#     q=PortalSnapshot.query
#     if snapshot is not None:
#         q=q.filter(PortalSnapshotQuality.snapshot==snapshot)
#     q=q.filter(PortalSnapshot.portalid==portalid)\
#         .outerjoin(PortalSnapshotQuality, and_(PortalSnapshot.portalid==PortalSnapshotQuality.portalid,PortalSnapshot.snapshot==PortalSnapshotQuality.snapshot))\
#         .join(Portal)\
#         .add_entity(PortalSnapshotQuality)\
#         .add_entity(Portal)
#
#     print 'Query',str(q)
#     return [row2dict(i) for i in q.all()]
#
# @ns.route('/<portalid>/<int:snapshot>/all', methods = ['GET'])
# @toJSON
# def portalSnapshotAll(snapshot,portalid):
#     return _portalAll(portalid, snapshot)
#
# @ns.route('/<portalid>/<int:snapshot>/all.csv', methods = ['GET'])
# @toCSV
# def portalSnapshotAllCSV(snapshot, portalid):
#     return _portalAll(portalid, snapshot)
#
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# ## PORTAL ALL
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# @ns.route('/<portalid>/all', methods = ['GET'])
# @toJSON
# def portalAll(portalid):
#     return _portalAll(portalid)
#
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# ## PORTAL SNAPSHOT QUALITY
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# @cache.cached(timeout=300)
# def _portalQuality(portalid, snapshot=None):
#     q=PortalSnapshotQuality.query
#     if snapshot:
#         q=q.filter(PortalSnapshotQuality.snapshot==snapshot)
#     q= q.filter(PortalSnapshotQuality.portalid==portalid)\
#         .all()
#     return [row2dict(i) for i in q]
#
# @ns.route('/<portalid>/<int:snapshot>/quality', methods = ['GET'])
# @toJSON
# def portalSnapshotQuality(snapshot,portalid):
#     return _portalQuality(portalid, snapshot)
#
# @ns.route('/<portalid>/<int:snapshot>/quality.csv', methods = ['GET'])
# @toCSV
# def portalSnapshotQualityCSV(snapshot, portalid):
#     return _portalQuality(portalid, snapshot)
#
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# ## PORTAL QUALITY
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# @ns.route('/<portalid>/quality', methods = ['GET'])
# @toJSON
# def portalQuality(portalid):
#     return _portalQuality(portalid)
#
#
#
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
# ## PORTAL SNAPSHOT RESOURCES
# #--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
#
# @cache.cached(timeout=300)
# def _portalResources(portalid, snapshot):
#     dbc=current_app.config['dbc']
#     return  [row2dict(i) for i in dbc.getResourceInfos(snapshot,portalid=portalid) ]
#
# @ns.route('/<portalid>/<int:snapshot>/resources.csv', methods = ['GET'])
# @toCSV
# def portalResourcesCSV(portalid,snapshot):
#     return _portalResources(portalid,snapshot)
#
#
# @ns.route('/<portalid>/<int:snapshot>/resources', methods = ['GET'])
# @crossdomain(origin='*',headers=['Content- Type','Authorization'])
# def portalResources(portalid,snapshot):
#     results={}
#     dbc=current_app.config['dbc']
#     for i in dbc.getResourceInfos(snapshot,portalid=portalid):
#         results= _portalResources(portalid,snapshot)
#
#
#
#     print results
#     return Response(json.dumps(results),  mimetype='application/json')
#