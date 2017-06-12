# -*- coding: utf-8 -*-
import json

from flask import Response
from odpw.core.dataset_converter import dict_to_dcat
from flask import make_response, request, current_app, jsonify
from flask_restplus import cors
from sqlalchemy import and_

from odpw.quality import dqv_export
from odpw.utils.datamonitor_utils import parseDate
from odpw.utils.timing import Timer
from odpw.web_rest.rest.odpw_restapi import api
from odpw.core.db import row2dict
from odpw.core.model import Portal, PortalSnapshotQuality, PortalSnapshot, ResourceInfo, MetaResource, Dataset, \
    DatasetData, DatasetQuality

import logging

from flask import request
from flask_restplus import Resource

from schemadotorg import dcat_to_schemadotorg

log = logging.getLogger(__name__)

ns = api.namespace('portal', description='Operations related to a specific portal')

@ns.route('/<portalid>/<int:snapshot>/all')
@ns.doc(params={'portalid': 'A portal id', 'snapshot':'Snapshot in yyww format (e.g. 1639 -> 2016 week 39)'}, description="This API returns the portal specific information and the aggregated quality measures for a given portal and snapshot")
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

        return jsonify([row2dict(i) for i in q.all()])


@ns.route('/<portalid>/<int:snapshot>/quality')
@ns.doc(params={'portalid': 'A portal id', 'snapshot':'Snapshot in yyww format (e.g. 1639 -> 2016 week 39)'}, description="This API returns the aggregated quality measures for a given portal and snapshot")
class PortalSnapshotQuality1(Resource):

    def get(self, portalid,snapshot):
        #print portalid, snapshot
        #with Timer(key="portalQuality",verbose=True):
        session=current_app.config['dbsession']

        q=session.query(PortalSnapshotQuality).filter(PortalSnapshotQuality.portalid==portalid).filter(PortalSnapshotQuality.snapshot==snapshot)
        data=[row2dict(r) for r in q.all()]

        return jsonify(data)


@ns.route('/<portalid>/<int:snapshot>/datasets')
@ns.doc(params={'portalid': 'A portal id', 'snapshot':'Snapshot in yyww format (e.g. 1639 -> 2016 week 39)'}, description="This API returns the full list of all datasets for a given portal and snapshot")
class PortalDatasets(Resource):

    #@cors.crossdomain(origin='*')
    def get(self, portalid,snapshot):
        with Timer(key="PortalDatasets.get",verbose=True):
            session=current_app.config['dbsession']

            q=session.query(Dataset)\
                .filter(Dataset.snapshot==snapshot)\
                .filter(Dataset.portalid==portalid)
            data=[row2dict(r) for r in q.all()]

            return jsonify(data)


@ns.route('/<portalid>/<int:snapshot>/dataset/<datasetid>')
@ns.doc(params={'portalid': 'A portal id', 'snapshot':'Snapshot in yyww format (e.g. 1639 -> 2016 week 30)','datasetid':'ID of dataset'})
class PortalDatasetData(Resource):

    #@cors.crossdomain(origin='*')
    def get(self, portalid,snapshot, datasetid):
        with Timer(key="PortalDatasetData.get",verbose=True):
            session=current_app.config['dbsession']

            q=session.query(DatasetData) \
                .join(Dataset, DatasetData.md5 == Dataset.md5) \
                .filter(Dataset.snapshot==snapshot)\
                .filter(Dataset.portalid==portalid)\
                .filter(Dataset.id == datasetid)
            data = [row2dict(r) for r in q.all()]

            return jsonify(data)


@ns.route('/<portalid>/<int:snapshot>/dataset/<datasetid>/dcat')
@ns.doc(params={'portalid': 'A portal id', 'snapshot':'Snapshot in yyww format (e.g. 1639 -> 2016 week 39)','datasetid':'ID of dataset'})
class PortalDatasetData(Resource):

    #@cors.crossdomain(origin='*')
    def get(self, portalid,snapshot, datasetid):
        with Timer(key="PortalDatasetData.get",verbose=True):
            session=current_app.config['dbsession']

            q=session.query(DatasetData) \
                .join(Dataset, DatasetData.md5 == Dataset.md5) \
                .filter(Dataset.snapshot==snapshot)\
                .filter(Dataset.portalid==portalid)\
                .filter(Dataset.id == datasetid)
            data=q.first()

            P= session.query(Portal).filter(Portal.id==portalid).first()
            return jsonify(dict_to_dcat(data.raw, P))
            #return jsonify(dict_to_dcat(data.raw, P))


@ns.route('/<portalid>/<int:snapshot>/dataset/<path:datasetid>/schemadotorg')
@ns.doc(params={'portalid': 'A portal id', 'snapshot':'Snapshot in yyww format (e.g. 1639 -> 2016 week 39)','datasetid':'ID of dataset'})
class PortalDatasetData(Resource):
    def get(self, portalid, snapshot, datasetid):
        with Timer(key="PortalDatasetData.get",verbose=True):
            session=current_app.config['dbsession']

            q=session.query(DatasetData) \
                .join(Dataset, DatasetData.md5 == Dataset.md5) \
                .filter(Dataset.snapshot==snapshot)\
                .filter(Dataset.portalid==portalid)\
                .filter(Dataset.id == datasetid)
            data=q.first()
            p = session.query(Portal).filter(Portal.id==portalid).first()
            doc = dcat_to_schemadotorg.convert(p, data.raw)
            return jsonify(doc)
            #return jsonify(dict_to_dcat(data.raw, P))




@ns.route('/<portalid>/<int:snapshot>/dataset/<datasetid>/quality')
@ns.doc(params={'portalid': 'A portal id', 'snapshot':'Snapshot in yyww format (e.g. 1639 -> 2016 week 39)','datasetid':'ID of dataset'})
class PortalDatasetDataQuality(Resource):

    #@cors.crossdomain(origin='*')
    def get(self, portalid,snapshot, datasetid):
        with Timer(key="PortalDatasetDataQuality.get",verbose=True):
            session=current_app.config['dbsession']

            q=session.query(DatasetQuality) \
                .join(Dataset, DatasetQuality.md5 == Dataset.md5) \
                .filter(Dataset.snapshot==snapshot)\
                .filter(Dataset.portalid==portalid)\
                .filter(Dataset.id == datasetid)
            data=[row2dict(r) for r in q.all()]

            return jsonify(data)


@ns.route('/<portalid>/<int:snapshot>/dataset/<datasetid>/dqv')
@ns.doc(params={'portalid': 'A portal id', 'snapshot':'Snapshot in yyww format (e.g. 1639 -> 2016 week 39)','datasetid':'ID of dataset'})
class PortalDatasetDataQuality(Resource):

    #@cors.crossdomain(origin='*')
    def get(self, portalid, snapshot, datasetid):
        with Timer(key="PortalDatasetDataQuality.get",verbose=True):
            session=current_app.config['dbsession']

            p = session.query(Portal).filter(Portal.id == portalid).first()

            q = session.query(DatasetQuality) \
                .join(Dataset, DatasetQuality.md5 == Dataset.md5) \
                .filter(Dataset.snapshot == snapshot) \
                .filter(Dataset.portalid == portalid) \
                .filter(Dataset.id == datasetid)
            dataset_qual = q.first()

            q=session.query(Dataset)\
                .filter(Dataset.snapshot==snapshot)\
                .filter(Dataset.portalid==portalid)\
                .filter(Dataset.id == datasetid)
            dataset = q.first()
            # get rdf graph and add measures and dimensions
            g = dqv_export.get_measures_for_dataset(p, dataset, dataset_qual)
            dqv_export.add_dimensions_and_metrics(g)
            return jsonify(json.loads(g.serialize(format="json-ld")))


@ns.route('/<portalid>/snapshots')
@ns.doc(params={'portalid': 'A portal id'})
class PortalSnapshots(Resource):

    #@cors.crossdomain(origin='*')
    def get(self, portalid):
        with Timer(key="PortalSnapshots.get",verbose=True):
            session=current_app.config['dbsession']

            q=session.query(PortalSnapshot.snapshot)\
                .filter(PortalSnapshot.portalid==portalid)
            data=[row2dict(r) for r in q.all()]

            return jsonify(data)

@ns.route('/<portalid>/<int:snapshot>/resources')
@ns.doc(params={'portalid': 'A portal id', 'snapshot': 'Snapshot in yyww format (e.g. 1639 -> 2016 week 30)', 'format': 'filter file format', 'size':'max file size, or None'})
class PortalSnapshotResources(Resource):
    # @cors.crossdomain(origin='*')
    def get(self, portalid, snapshot):
        with Timer(key="PortalSnapshotResources.get", verbose=True):
            session = current_app.config['dbsession']

            q = session.query(MetaResource.uri) \
                .join(Dataset, Dataset.md5 == MetaResource.md5) \
                .filter(Dataset.snapshot == snapshot) \
                .filter(Dataset.portalid == portalid)

            format = request.args.get("format")
            if format:
                q = q.filter(MetaResource.format == format)

            size = request.args.get("size")
            if size:
                q = q.filter((MetaResource.size <= size) | (MetaResource.size == None))

            data = [row2dict(r)['uri'] for r in q.all()]

            return jsonify(data)

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
