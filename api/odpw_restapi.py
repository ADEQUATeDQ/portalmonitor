# -*- coding: utf-8 -*-
import StringIO
import csv
import json
import logging

import rdflib
from rdflib.namespace import Namespace, RDF

from odpw.quality.dcat_analysers import dcat_analyser
from odpw.quality.dqv_export import dataset_quality_to_dqv
from odpw.utils import utils_snapshot

DCAT = Namespace("http://www.w3.org/ns/dcat#")

import requests
from flask import jsonify, make_response
from flask_restplus import Resource
from werkzeug.utils import secure_filename

from api import parsers
from odpw.core import dataset_converter
from odpw.core.dataset_converter import add_dcat_to_graph

from odpw.quality import dqv_export
from schemadotorg import dcat_to_schemadotorg

log = logging.getLogger(__name__)


from flask_restplus import Api
api = Api(version='1.0', title='ODPW Rest API', description='ODPW')


@api.errorhandler
def default_error_handler(error):
    '''Default error handler'''
    return {'message': str(error)}, getattr(error, 'code', 500)


def makeResponse(resp, filename, content_type='application/json'):
    output = make_response(resp)
    output.headers["Content-Disposition"] = "attachment; filename=" + filename
    output.headers["Content-Type"] = content_type+";charset=utf-8"
    return output


conv = api.namespace('mapping', description='Metadata mapping to DCAT and Schema.org')

@conv.route('/dcat')
class DCATConversion(Resource):

    def _get_dcat(self, args, data, filename):
        try:
            default_url = 'http://missing.portal.url.com'
            portal_url = args.get('portal_url', default_url)
            if not portal_url:
                portal_url = default_url

            if 'software' in args:
                software = args['software']
                g = rdflib.Graph()

                # stub portal class
                class Portal:
                    def __init__(self, software, apiuri):
                        self.software = software
                        self.apiuri = apiuri

                p = Portal(software, portal_url)

                dataset_ref = add_dcat_to_graph(data, p, g, None)
                resp = jsonify(json.loads(g.serialize(format='json-ld')))
                filename = secure_filename(filename).split('/')[-1]
                return makeResponse(resp, filename)
            else:
                e = 'Portal software parameter required for conversion. ' \
                    '"software" should be "CKAN", "Socrata", or "OpenDataSoft".'
        except Exception as ex:
            e = ex.message

        resp = jsonify({
            'error': 'Could not parse JSON',
            'message': e
        })
        resp.status_code = 406
        return resp


    @conv.expect(parsers.post_param)
    def post(self):
        args = parsers.post_param.parse_args()

        if args['json_file'].mimetype == 'application/json':
            try:
                file = args['json_file']
                data = json.load(file)
                return self._get_dcat(args, data, file.filename)
            except Exception as ex:
                e = ex.message
        else:
            e = 'Wrong mime-type of uploaded file. ''application/json'' expected.'

        resp = jsonify({
            'error': 'Could not parse JSON',
            'message': e
        })
        resp.status_code = 406
        return resp


    @conv.expect(parsers.url_param)
    def get(self):
        args = parsers.url_param.parse_args()

        if 'url' in args:
            try:
                url = args['url']
                req = requests.get(url)
                data = req.json()
                return self._get_dcat(args, data, url)
            except Exception as ex:
                e = ex.message
        else:
            e = 'Metadata URL parameter required for conversion. ' \
                '"url" parameter should provide the JSON metadata document.'

        resp = jsonify({
            'error': 'Could not parse JSON',
            'message': e
        })
        resp.status_code = 406
        return resp



@conv.route('/schema')
class SchemaConversion(Resource):

    def _get_schema(self, args, data, filename):
        try:
            default_url = 'http://missing.portal.url.com'
            portal_url = args.get('portal_url', default_url)
            if not portal_url:
                portal_url = default_url

            default_country = 'undefined'
            country = args.get('country', default_country)
            if not country:
                country = default_country

            if 'software' in args:
                software = args['software']
                g = rdflib.Graph()

                # stub portal class
                class Portal:
                    def __init__(self, software, uri, iso):
                        self.software = software
                        self.apiuri = uri
                        self.iso = iso
                        self.uri = uri

                p = Portal(software, portal_url, country)

                doc = dcat_to_schemadotorg.convert(p, data)
                resp = jsonify(doc)
                filename = secure_filename(filename).split('/')[-1]
                return makeResponse(resp, filename)
            else:
                e = 'Portal software parameter required for conversion. ' \
                    '"software" should be "CKAN", "Socrata", or "OpenDataSoft".'
        except Exception as ex:
            e = ex.message

        resp = jsonify({
            'error': 'Could not parse JSON',
            'message': e
        })
        resp.status_code = 406
        return resp


    @conv.expect(parsers.post_param)
    def post(self):
        args = parsers.post_param.parse_args()

        if args['json_file'].mimetype == 'application/json':
            try:
                file = args['json_file']
                data = json.load(file)
                return self._get_schema(args, data, file.filename)
            except Exception as ex:
                e = ex.message
        else:
            e = 'Wrong mime-type of uploaded file. ''application/json'' expected.'

        resp = jsonify({
            'error': 'Could not parse JSON',
            'message': e
        })
        resp.status_code = 406
        return resp


    @conv.expect(parsers.url_param)
    def get(self):
        args = parsers.url_param.parse_args()

        if 'url' in args:
            try:
                url = args['url']
                req = requests.get(url)
                data = req.json()
                return self._get_schema(args, data, url)
            except Exception as ex:
                e = ex.message
        else:
            e = 'Metadata URL parameter required for conversion. ' \
                '"url" parameter should provide the JSON metadata document.'

        resp = jsonify({
            'error': 'Could not parse JSON',
            'message': e
        })
        resp.status_code = 406
        return resp


qa = api.namespace('quality', description='Quality assessment of metadata descriptions')

class DatasetQuality:
    def __init__(self, data, dcat):
        # dataset stub
        class Dataset:
            def __init__(self, data, dcat):
                self.data = data
                self.dcat = dcat
        dataset = Dataset(data, dcat)

        for id, qa in dcat_analyser().items():
            self.__dict__[qa.id.lower()] = qa.analyse_Dataset(dataset)


@qa.route('/qa')
class Quality(Resource):
    def _get_quality(self, args, data, filename):
        try:
            content_type = 'application/json'
            default_url = 'http://missing.portal.url.com'
            portal_url = args.get('portal_url', default_url)
            if not portal_url:
                portal_url = default_url

            default_out = 'json'
            out_format = args.get('format', default_out)
            if not out_format:
                out_format = default_out

            filter_metrics = args.get('metric')

            if 'software' in args:
                software = args['software']

                # stub portal class
                class Portal:
                    def __init__(self, software, uri):
                        self.software = software
                        self.apiuri = uri

                p = Portal(software, portal_url)

                # get rdf graph and add measures and dimensions
                graph = rdflib.Graph()
                # write dcat dataset into graph
                dcat = dataset_converter.dict_to_dcat(data, p, graph=graph)
                measures_g = rdflib.Graph()
                ds_id = graph.value(predicate=RDF.type, object=DCAT.Dataset)
                datasetquality = DatasetQuality(data, dcat)
                metrics_dict = datasetquality.__dict__

                if filter_metrics:
                    metrics_dict = {m: metrics_dict[m] for m in filter_metrics}

                if out_format == 'json':
                    resp = jsonify(metrics_dict)
                elif out_format == 'json-ld':
                    dataset_quality_to_dqv(measures_g, ds_id, datasetquality, utils_snapshot.getCurrentSnapshot())
                    dqv_export.add_dimensions_and_metrics(measures_g)
                    resp = jsonify(json.loads(measures_g.serialize(format="json-ld")))
                elif out_format == 'csv':
                    outstr = StringIO.StringIO()
                    w = csv.DictWriter(outstr, metrics_dict.keys())
                    w.writeheader()
                    w.writerow(metrics_dict)
                    resp = outstr.getvalue()
                    content_type = 'text/csv'
                else:
                    raise Exception('output format not supported: ' + out_format)

                filename = secure_filename(filename).split('/')[-1]
                return makeResponse(resp, filename, content_type=content_type)
            else:
                e = 'Portal software parameter required for conversion. ' \
                    '"software" should be "CKAN", "Socrata", or "OpenDataSoft".'
        except Exception as ex:
            e = ex.message

        resp = jsonify({
            'error': 'Could not parse JSON',
            'message': e
        })
        resp.status_code = 406
        return resp


    @conv.expect(parsers.qual_post)
    def post(self):
        args = parsers.qual_post.parse_args()

        if args['json_file'].mimetype == 'application/json':
            try:
                file = args['json_file']
                data = json.load(file)
                return self._get_quality(args, data, file.filename)
            except Exception as ex:
                e = ex.message
        else:
            e = 'Wrong mime-type of uploaded file. ''application/json'' expected.'

        resp = jsonify({
            'error': 'Could not parse JSON',
            'message': e
        })
        resp.status_code = 406
        return resp


    @conv.expect(parsers.qual_url)
    def get(self):
        args = parsers.qual_url.parse_args()

        if 'url' in args:
            try:
                url = args['url']
                req = requests.get(url)
                data = req.json()
                return self._get_quality(args, data, url)
            except Exception as ex:
                e = ex.message
        else:
            e = 'Metadata URL parameter required for conversion. ' \
                '"url" parameter should provide the JSON metadata document.'

        resp = jsonify({
            'error': 'Could not parse JSON',
            'message': e
        })
        resp.status_code = 406
        return resp

