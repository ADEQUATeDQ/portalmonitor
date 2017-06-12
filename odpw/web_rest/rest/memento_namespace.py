# -*- coding: utf-8 -*-
import json

import datetime
import rdflib
from flask import Response
from odpw.core.dataset_converter import dict_to_dcat, add_dcat_to_graph
from flask import make_response, request, current_app, jsonify
from flask_restplus import cors
from sqlalchemy import and_

from odpw.quality import dqv_export
from odpw.utils.datamonitor_utils import parseDate
from odpw.utils.timing import Timer
from odpw.utils.utils_snapshot import getCurrentSnapshot, getSnapshotfromTime, tofirstdayinisoweek
from odpw.web_rest.rest.odpw_restapi import api
from odpw.core.db import row2dict
from odpw.core.model import Portal, PortalSnapshotQuality, PortalSnapshot, ResourceInfo, MetaResource, Dataset, \
    DatasetData, DatasetQuality


from flask import request
from flask_restplus import Resource, fields
from schemadotorg import dcat_to_schemadotorg

from email.utils import formatdate
import email.utils as eut
from time import mktime

import logging
log = logging.getLogger(__name__)

ns = api.namespace('memento', description='Operations to retrieve historical dataset information')
HOST = 'http://data.wu.ac.at/portalwatch/api/v1/memento'

def add_memento_header(resp, orig_ref, timegate, snapshot):
    # timestamp of snapshot
    dt = tofirstdayinisoweek(snapshot)
    stamp = mktime(dt.timetuple())
    formatted_dt = formatdate(
        timeval=stamp,
        localtime=False,
        usegmt=True
    )
    resp.headers['Memento-Datetime'] = formatted_dt
    # link to original resource
    resp.headers['Link'] = orig_ref + '; rel="original", ' + timegate + '; rel="timegate"'
    return resp

def parse_rfc1123(text):
    return datetime.datetime(*eut.parsedate(text)[:6])


####################### ORIGINAL ##################################
def get_dataset(portalid, snapshot, datasetid):
    session = current_app.config['dbsession']

    q = session.query(DatasetData) \
        .join(Dataset, DatasetData.md5 == Dataset.md5) \
        .filter(Dataset.snapshot <= snapshot) \
        .filter(Dataset.portalid == portalid) \
        .filter(Dataset.id == datasetid) \
        .order_by(Dataset.snapshot.desc())
    data = q.first()
    if data:
        resp = jsonify(row2dict(data))
        portal = session.query(Portal).filter(Portal.id == portalid).first()
        g = rdflib.Graph()
        dataset_ref = add_dcat_to_graph(data.raw, portal, g, None)
        timegate = '<' + HOST + '/' + portalid + '/' + datasetid + '>'
        return add_memento_header(resp, dataset_ref.n3(), timegate, snapshot)
    else:
        resp = jsonify(
            {'error': 'There is no version of dataset ' + datasetid + ' available that is older than ' + str(tofirstdayinisoweek(snapshot)),
             'portalid': portalid})
        resp.status_code = 404
        return resp


model = api.model('TimeGate', {
    'Vary': fields.String,
})

@ns.route('/<portalid>/<datasetid>')
@api.header('Accept-Datetime', 'Retrieve archived versions', required=False)
@api.response(200, 'The requested dataset is available and was successfully returned.')
@api.response(404, 'There is no version of the specified datasetid available that is older than the given date.')
@ns.doc(params={'portalid': 'ID of the data portal', 'datasetid':'ID of the dataset'})
class PortalDatasetData(Resource):
    def get(self, portalid, datasetid):
        if request.headers.get('Accept-Datetime'):
            acc_dt = request.headers['Accept-Datetime']
            sn = getSnapshotfromTime(parse_rfc1123(acc_dt))
        else:
            sn = getCurrentSnapshot()

        resp = get_dataset(portalid, sn, datasetid)
        resp.headers['Vary'] = 'accept-datetime'
        d = tofirstdayinisoweek(sn)
        full_url = HOST + '/' + portalid + '/' + d.strftime("%Y%m%d") + '/' + datasetid
        resp.headers['Content-Location'] = full_url
        return resp



@ns.route('/<portalid>/<date>/<datasetid>')
@api.response(200, 'The requested dataset is available and was successfully returned.')
@api.response(404, 'There is no version of the specified datasetid available that is older than the given date.')
@ns.doc(params={'portalid': 'ID of the data portal', 'date':'Date as \"YYYY<MM|DD|HH|MM|SS>\"', 'datasetid':'ID of the dataset'})
class PortalDatasetData(Resource):
    def get(self, portalid, date, datasetid):
        d = parseDate(date)
        sn = getSnapshotfromTime(d)
        resp = get_dataset(portalid, sn, datasetid)
        return resp



######################## DCAT ######################################
def get_dcat(portalid, datasetid, snapshot):
    session = current_app.config['dbsession']

    q = session.query(DatasetData) \
        .join(Dataset, DatasetData.md5 == Dataset.md5) \
        .filter(Dataset.snapshot <= snapshot) \
        .filter(Dataset.portalid == portalid) \
        .filter(Dataset.id == datasetid) \
        .order_by(Dataset.snapshot.desc())
    data = q.first()

    if data:
        portal = session.query(Portal).filter(Portal.id == portalid).first()
        g = rdflib.Graph()
        dataset_ref = add_dcat_to_graph(data.raw, portal, g, None)
        resp = jsonify(json.loads(g.serialize(format='json-ld')))
        timegate = '<' + HOST + '/' + portalid + '/' + datasetid + '/dcat>'
        return add_memento_header(resp, dataset_ref.n3(), timegate, snapshot)
    else:
        resp = jsonify(
            {'error': 'There is no version of dataset ' + datasetid + ' available that is older than ' + str(tofirstdayinisoweek(snapshot)),
             'portalid': portalid})
        resp.status_code = 404
        return resp


@ns.route('/<portalid>/<datasetid>/dcat')
@api.header('Accept-Datetime', 'Retrieve archived versions', required=False)
@api.response(200, 'The requested dataset is available and was successfully returned.')
@api.response(404, 'There is no version of the specified datasetid available that is older than the given date.')
@ns.doc(params={'portalid': 'ID of the data portal', 'datasetid':'ID of the dataset'})
class PortalDatasetData(Resource):
    def get(self, portalid, datasetid):
        if request.headers.get('Accept-Datetime'):
            acc_dt = request.headers['Accept-Datetime']
            sn = getSnapshotfromTime(parse_rfc1123(acc_dt))
        else:
            sn = getCurrentSnapshot()

        resp = get_dcat(portalid, datasetid, sn)
        resp.headers['Vary'] = 'accept-datetime'
        d = tofirstdayinisoweek(sn)
        full_url = HOST + '/' + portalid + '/' + d.strftime("%Y%m%d") + '/' + datasetid + '/dcat'
        resp.headers['Content-Location'] = full_url
        return resp


@ns.route('/<portalid>/<date>/<datasetid>/dcat')
@api.response(200, 'The requested dataset is available and was successfully returned.')
@api.response(404, 'There is no version of the specified datasetid available that is older than the given date.')
@ns.doc(params={'portalid': 'ID of the data portal', 'date':'Date as \"YYYY<MM|DD|HH|MM|SS>\"', 'datasetid':'ID of the dataset'})
class PortalDatasetData(Resource):
    def get(self, portalid, date, datasetid):
        d = parseDate(date)
        sn = getSnapshotfromTime(d)
        return get_dcat(portalid, datasetid, sn)


################################### Schema.org ############################################
@ns.route('/<portalid>/<datasetid>/schemadotorg')
@api.header('Accept-Datetime', 'Retrieve archived versions', required=False)
@ns.doc(params={'portalid': 'ID of the data portal', 'datasetid':'ID of the dataset'})
class PortalDatasetData(Resource):
    def get(self, portalid, datasetid):
        if request.headers.get('Accept-Datetime'):
            acc_dt = request.headers['Accept-Datetime']
            sn = getSnapshotfromTime(parse_rfc1123(acc_dt))
        else:
            sn = getCurrentSnapshot()

        session = current_app.config['dbsession']

        q = session.query(DatasetData) \
            .join(Dataset, DatasetData.md5 == Dataset.md5) \
            .filter(Dataset.snapshot == sn) \
            .filter(Dataset.portalid == portalid) \
            .filter(Dataset.id == datasetid)
        data = q.first()
        p = session.query(Portal).filter(Portal.id == portalid).first()
        doc = dcat_to_schemadotorg.convert(p, data.raw)
        timegate = '<' + HOST + '/' + portalid + '/' + datasetid + '/schemadotorg>'
        resp = add_memento_header(jsonify(doc), '<' + doc['@id'] + '>', timegate, sn)

        resp.headers['Vary'] = 'accept-datetime'
        d = tofirstdayinisoweek(sn)
        full_url = '<' + HOST + '/' + portalid + '/' + d.strftime("%y%m%d") + '/' + datasetid + '/schemadotorg>'
        resp.headers['Content-Location'] = full_url
        return resp

@ns.route('/<portalid>/<date>/<datasetid>/schemadotorg')
@api.response(200, 'The requested dataset is available and was successfully returned.')
@api.response(404, 'There is no version of the specified datasetid available that is older than the given date.')
@ns.doc(params={'portalid': 'ID of the data portal', 'date':'Date as \"YYYY<MM|DD|HH|MM|SS>\"', 'datasetid':'ID of the dataset'})
class PortalDatasetData(Resource):
    def get(self, portalid, date, datasetid):
        d = parseDate(date)
        sn = getSnapshotfromTime(d)

        session=current_app.config['dbsession']
        q=session.query(DatasetData) \
            .join(Dataset, DatasetData.md5 == Dataset.md5) \
            .filter(Dataset.snapshot<=sn)\
            .filter(Dataset.portalid==portalid)\
            .filter(Dataset.id == datasetid) \
            .order_by(Dataset.snapshot.desc())
        data = q.first()

        if data:
            p = session.query(Portal).filter(Portal.id == portalid).first()
            doc = dcat_to_schemadotorg.convert(p, data.raw)
            timegate = '<' + HOST + '/' + portalid + '/' + datasetid + '/schemadotorg>'
            return add_memento_header(jsonify(doc), '<' + doc['@id'] + '>', timegate, sn)
        else:
            resp = jsonify({'error': 'There is no version of dataset ' + datasetid + ' available that is older than ' + str(d), 'portalid': portalid})
            resp.status_code = 404
            return resp



############################## quality measures ###################################
@ns.route('/<portalid>/<datasetid>/dqv')
@api.header('Accept-Datetime', 'Retrieve archived versions', required=False)
@ns.doc(params={'portalid': 'ID of the data portal', 'datasetid':'ID of the dataset'})
class PortalDatasetData(Resource):
    def get(self, portalid, datasetid):
        if request.headers.get('Accept-Datetime'):
            acc_dt = request.headers['Accept-Datetime']
            sn = getSnapshotfromTime(parse_rfc1123(acc_dt))
        else:
            sn = getCurrentSnapshot()


        session = current_app.config['dbsession']
        p = session.query(Portal).filter(Portal.id == portalid).first()
        q = session.query(DatasetQuality) \
            .join(Dataset, DatasetQuality.md5 == Dataset.md5) \
            .filter(Dataset.snapshot == sn) \
            .filter(Dataset.portalid == portalid) \
            .filter(Dataset.id == datasetid)
        dataset_qual = q.first()

        q = session.query(Dataset) \
            .filter(Dataset.snapshot == sn) \
            .filter(Dataset.portalid == portalid) \
            .filter(Dataset.id == datasetid)
        dataset = q.first()
        # get rdf graph and add measures and dimensions
        g, ds_id = dqv_export._get_measures_for_dataset(p, dataset, dataset_qual)
        dqv_export.add_dimensions_and_metrics(g)
        resp = jsonify(json.loads(g.serialize(format="json-ld")))
        timegate = '<' + HOST + '/' + portalid + '/' + datasetid + '/dqv>'
        resp = add_memento_header(resp, ds_id.n3(), timegate, sn)

        resp.headers['Vary'] = 'accept-datetime'
        d = tofirstdayinisoweek(sn)
        full_url = '<' + HOST + '/' + portalid + '/' + d.strftime("%y%m%d") + '/' + datasetid + '/dqv>'
        resp.headers['Content-Location'] = full_url
        return resp



@ns.route('/<portalid>/<date>/<datasetid>/dqv')
@ns.doc(params={'portalid': 'ID of the data portal', 'date':'Date as \"YYYY<MM|DD|HH|MM|SS>\"', 'datasetid':'ID of the dataset'})
class PortalDatasetDataQuality(Resource):
    def get(self, portalid, date, datasetid):
        d = parseDate(date)
        sn = getSnapshotfromTime(d)

        session=current_app.config['dbsession']
        p = session.query(Portal).filter(Portal.id == portalid).first()

        q = session.query(Dataset) \
            .filter(Dataset.snapshot <= sn) \
            .filter(Dataset.portalid == portalid) \
            .filter(Dataset.id == datasetid) \
            .order_by(Dataset.snapshot.desc())
        dataset = q.first()

        if dataset:
            snapshot = dataset.snapshot

            q = session.query(DatasetQuality) \
                .join(Dataset, DatasetQuality.md5 == Dataset.md5) \
                .filter(Dataset.snapshot == snapshot) \
                .filter(Dataset.portalid == portalid) \
                .filter(Dataset.id == datasetid)
            dataset_qual = q.first()

            # get rdf graph and add measures and dimensions
            g, ds_id = dqv_export._get_measures_for_dataset(p, dataset, dataset_qual)
            dqv_export.add_dimensions_and_metrics(g)
            resp = jsonify(json.loads(g.serialize(format="json-ld")))
            timegate = '<' + HOST + '/' + portalid + '/' + datasetid + '/dqv>'
            return add_memento_header(resp, ds_id.n3(), timegate, snapshot)
        else:
            return jsonify({'error': 'There is no version of dataset ' + datasetid + ' available that is older than ' + str(d),
                        'portalid': portalid})
