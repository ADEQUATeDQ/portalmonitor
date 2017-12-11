# -*- coding: utf-8 -*-
import hashlib
import json
import datetime
import re
import urlparse
import uuid

from dateutil.parser import parse as parse_date

import rdflib
from rdflib import URIRef, BNode, Literal

from rdflib.namespace import Namespace, RDF, XSD, SKOS, RDFS

from geomet import wkt, InvalidGeoJSONException
from rfc3987 import get_compiled_pattern

DCT = Namespace("http://purl.org/dc/terms/")
DCAT = Namespace("http://www.w3.org/ns/dcat#")
ADMS = Namespace("http://www.w3.org/ns/adms#")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
SCHEMA = Namespace('http://schema.org/')
TIME = Namespace('http://www.w3.org/2006/time')
LOCN = Namespace('http://www.w3.org/ns/locn#')
GSP = Namespace('http://www.opengis.net/ont/geosparql#')
OWL = Namespace('http://www.w3.org/2002/07/owl#')
ADEQUATE = Namespace('https://www.adequate.at/ns#')

GEOJSON_IMT = 'https://www.iana.org/assignments/media-types/application/vnd.geo+json'

namespaces = {
    'dct': DCT,
    'dcat': DCAT,
    'adms': ADMS,
    'vcard': VCARD,
    'foaf': FOAF,
    'schema': SCHEMA,
    'time': TIME,
    'skos': SKOS,
    'locn': LOCN,
    'gsp': GSP,
    'owl': OWL,
}


def add_dcat_to_graph(dataset_dict, portal, graph, portal_uri):
    dataset_ref = None

    if portal.software == 'CKAN':
        converter = CKANConverter(graph, portal.apiuri)
        dataset_ref = converter.graph_from_ckan(dataset_dict)
    elif portal.software == 'Socrata':
        if 'dcat' in dataset_dict and dataset_dict['dcat']:
            graph.parse(data=dataset_dict['dcat'], format='xml')
            dataset_ref = fix_socrata_graph(graph, dataset_dict, portal.apiuri)
            # TODO redesign distribution, format, contact (publisher, organization)
    elif portal.software == 'OpenDataSoft':
        dataset_ref = graph_from_opendatasoft(graph, dataset_dict, portal.apiuri)
        # TODO contact, publisher, organization
    elif portal.software == 'XMLDCAT':
        # dataset_dict is already json-ld
        str_ds = json.dumps(dataset_dict)
        graph.parse(data=str_ds, format='json-ld')
        dataset_ref = graph.value(predicate=RDF.type, object=DCAT.Dataset)

    # add portal ref to graph
    if portal_uri and dataset_ref:
        graph.add((portal_uri, DCAT.dataset, dataset_ref))
    return dataset_ref


def dict_to_dcat(dataset_dict, portal, graph=None, format='json-ld', portal_uri=None):
    if graph == None:
        graph = rdflib.Graph()
    dataset_ref = None

    if portal.software == 'CKAN':
        converter = CKANConverter(graph, portal.apiuri)
        dataset_ref = converter.graph_from_ckan(dataset_dict)
    elif portal.software == 'Socrata':
        if 'dcat' in dataset_dict and dataset_dict['dcat']:
            graph.parse(data=dataset_dict['dcat'], format='xml')
            dataset_ref = fix_socrata_graph(graph, dataset_dict, portal.apiuri)
            # TODO redesign distribution, format, contact (publisher, organization)
    elif portal.software == 'OpenDataSoft':
        dataset_ref = graph_from_opendatasoft(graph, dataset_dict, portal.apiuri)
        # TODO contact, publisher, organization
    elif portal.software == 'XMLDCAT':
        # dataset_dict is already json-ld
        str_ds = json.dumps(dataset_dict)
        graph.parse(data=str_ds, format='json-ld')
        dataset_ref = graph.value(predicate=RDF.type, object=DCAT.Dataset)

    # add portal ref to graph
    if portal_uri and dataset_ref:
        graph.add((portal_uri, DCAT.dataset, dataset_ref))

    if portal.software == 'XMLDCAT':
        return dataset_dict
    else:
        return json.loads(graph.serialize(format=format))


def fix_socrata_graph(g, dataset_dict, portal_url):
    dataset_ref = None
    # add additional info
    if 'view' in dataset_dict and isinstance(dataset_dict['view'], dict):
        data = dataset_dict['view']
        try:
            identifier = data['id']
            uri = '{0}/dataset/{1}'.format(portal_url.rstrip('/'), identifier)
            dataset_ref = URIRef(uri)
            # replace blank node by dataset reference
            dataset_node = g.value(predicate=DCT.identifier, object=Literal(identifier))
            if dataset_node:
                for s, p, o in g.triples( (dataset_node, None, None) ):
                    g.remove((s, p, o))
                    g.add((dataset_ref, p, o))
            if (dataset_ref, RDF.type, DCAT.Dataset) not in g:
                g.add((dataset_ref, RDF.type, DCAT.Dataset))

            # owner
            if 'owner' in data and isinstance(data['owner'], dict) and 'displayName' in data['owner']:
                owner = data['owner']['displayName']
                # add owner as publisher
                # BNode: dataset_ref + DCT.publisher + owner
                bnode_hash = hashlib.sha1(dataset_ref.n3() + DCT.publisher.n3() + owner)
                publisher_details = BNode(bnode_hash.hexdigest())

                g.add((publisher_details, RDF.type, FOAF.Organization))
                g.add((dataset_ref, DCT.publisher, publisher_details))
                g.add((publisher_details, FOAF.name, Literal(owner)))
            # author
            if 'tableAuthor' in data and isinstance(data['tableAuthor'], dict) and 'displayName' in data['tableAuthor']:
                author = data['tableAuthor']['displayName']
                # BNode: dataset_ref + VCARD.fn + author
                bnode_hash = hashlib.sha1(dataset_ref.n3() + VCARD.fn.n3() + author)
                contact_details = BNode(bnode_hash.hexdigest())

                g.add((contact_details, RDF.type, VCARD.Organization))
                g.add((dataset_ref, DCAT.contactPoint, contact_details))
                g.add((contact_details, VCARD.fn, Literal(author)))

            #title
            #if 'name' in data:

            #    g.add((dataset_ref, DCT.title, Literal('name')))

        except Exception as e:
            pass
        try:
            # redesign distribution, format
            for ds, has_distr, dcat_download in g.triples((None, DCAT.distribution, None)):

                # create new distr
                # BNode: dataset_ref + dcat_download
                bnode_hash = hashlib.sha1(dataset_ref.n3() + dcat_download)
                distribution = BNode(bnode_hash.hexdigest())

                # rewrite format
                for s, p, format_bnode in g.triples((dcat_download, DCT['format'], None)):
                    format = g.value(format_bnode, RDFS.label)
                    mime_type = g.value(format_bnode, RDF.value)
                    # keep the blank node and add type MediaTypeOrExtent
                    #g.remove((format_bnode, None, None))
                    #g.add((distribution, DCT['format'], format))
                    g.add((format, RDF.type, DCT.MediaTypeOrExtent))
                    # additionally add media type to distribution
                    g.add((distribution, DCAT.mediaType, mime_type))
                    g.remove((s, p, format_bnode))

                # add new distr
                g.add((ds, DCAT.distribution, distribution))
                g.add((distribution, RDF.type, DCAT.Distribution))
                # remove old dcat:Download
                g.remove((ds, has_distr, dcat_download))
                g.remove((dcat_download, RDF.type, None))
                # add links from old distribution
                for s, p, o in g.triples((dcat_download, None, None)):
                    g.remove((s, p, o))
                    g.add((distribution, p, o))

        except Exception as e:
            pass

        try:
            # created, modified keys
            ODS_created = URIRef('http://open-data-standards.github.com/2012/01/open-data-standards#created')
            ODS_modified = URIRef('http://open-data-standards.github.com/2012/01/open-data-standards#last_modified')
            for s, p, o in g.triples((None, ODS_created, None)):
                g.remove((s, p, o))
                g.add((s, DCT.issued, o))
            for s, p, o in g.triples((None, ODS_modified, None)):
                g.remove((s, p, o))
                g.add((s, DCT.modified, o))

        except Exception as e:
            pass
    if not dataset_ref:
        dataset_ref = g.value(predicate=RDF.type, object=DCAT.Dataset)
    return dataset_ref

# TODO disallows whitespaces
VALID_URL = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

URI = get_compiled_pattern('^%(URI)s$')


def is_valid_url(references):
    try:
        res = urlparse.urlparse(references)
        return bool(res.scheme and res.netloc)
    except Exception as e:
        return False


def is_valid_uri(references):
    return bool(URI.match(references))


def graph_from_opendatasoft(g, dataset_dict, portal_url):
    # available: title, description, language, theme, keyword, license, publisher, references
    # additional: created, issued, creator, contributor, accrual periodicity, spatial, temporal, granularity, data quality

    identifier = dataset_dict['datasetid']
    uri = '{0}/explore/dataset/{1}'.format(portal_url.rstrip('/'), identifier)

    # dataset subject
    dataset_ref = URIRef(uri)
    for prefix, namespace in namespaces.iteritems():
        g.bind(prefix, namespace)

    g.add((dataset_ref, RDF.type, DCAT.Dataset))

    # identifier
    g.add((dataset_ref, DCT.identifier, Literal(identifier)))
    data = dataset_dict['metas']
    # Basic fields
    items = [
        ('title', DCT.title, None),
        ('description', DCT.description, None),
    ]
    _add_triples_from_dict(g, data, dataset_ref, items)

    #  Lists
    items = [
        ('language', DCT.language, None),
        ('theme', DCAT.theme, None),
        ('keyword', DCAT.keyword, None),
    ]
    _add_list_triples_from_dict(g, data, dataset_ref, items)

    # publisher
    publisher_name = data.get('publisher')
    if publisher_name:
        # BNode: dataset_ref + DCT.publisher + publisher_name
        bnode_hash = hashlib.sha1(dataset_ref.n3() + DCT.publisher.n3() + publisher_name)
        publisher_details = BNode(bnode_hash.hexdigest())

        g.add((publisher_details, RDF.type, FOAF.Organization))
        g.add((dataset_ref, DCT.publisher, publisher_details))
        g.add((publisher_details, FOAF.name, Literal(publisher_name)))
        # TODO any additional publisher information available? look for fields

    # Dates
    items = [
        #('metadata_processed', DCT.issued, ['metadata_created']),
        ('modified', DCT.modified, ['metadata_processed', 'metadata_modified']),
    ]
    _add_date_triples_from_dict(g, data, dataset_ref, items)

    # references
    references = data.get('references')
    if references and isinstance(references, basestring) and bool(urlparse.urlparse(references).netloc):
        references = references.strip()
        if is_valid_uri(references):
            g.add((dataset_ref, RDFS.seeAlso, URIRef(references)))
        else:
            g.add((dataset_ref, RDFS.seeAlso, Literal(references)))

    # store licenses for distributions
    license = data.get('license')

    # distributions
    if dataset_dict.get('has_records'):
        exports = [('csv', 'text/csv'), ('json', 'application/json'), ('xls', 'application/vnd.ms-excel')]
        if 'geo' in dataset_dict.get('features', []):
            exports.append(('geojson', 'application/vnd.geo+json'))
            exports.append(('kml', 'application/vnd.google-earth.kml+xml'))
            # TODO shape files?
            # exports.append(('shp', 'application/octet-stream'))
        for format, mimetype in exports:
            # URL
            url = portal_url.rstrip('/') + '/api/records/1.0/download?dataset=' + identifier + '&format=' + format

            # BNode: dataset_ref + url
            id_string = dataset_ref.n3() + url
            bnode_hash = hashlib.sha1(id_string.encode('utf-8'))
            distribution = BNode(bnode_hash.hexdigest())

            g.add((dataset_ref, DCAT.distribution, distribution))
            g.add((distribution, RDF.type, DCAT.Distribution))

            if is_valid_uri(url):
                g.add((distribution, DCAT.accessURL, URIRef(url)))
            else:
                g.add((distribution, DCAT.accessURL, Literal(url)))

            # License
            if license:
                # BNode: distribution + url
                id_string = distribution.n3() + license
                bnode_hash = hashlib.sha1(id_string.encode('utf-8'))
                l = BNode(bnode_hash.hexdigest())

                g.add((distribution, DCT.license, l))
                g.add((l, RDF.type, DCT.LicenseDocument))
                g.add((l, RDFS.label, Literal(license)))

            # Format
            # BNode: distribution + format + mimetype
            id_string = distribution.n3() + format + mimetype
            bnode_hash = hashlib.sha1(id_string.encode('utf-8'))
            f = BNode(bnode_hash.hexdigest())

            g.add((distribution, DCT['format'], f))
            g.add((f, RDF.type, DCT.MediaTypeOrExtent))
            g.add((f, RDFS.label, Literal(format)))
            g.add((f, RDF.value, Literal(mimetype)))
            g.add((distribution, DCAT.mediaType, Literal(mimetype)))


            # Dates
            items = [
                #('issued', DCT.issued, None),
                ('data_processed', DCT.modified, None),
            ]
            _add_date_triples_from_dict(g, data, distribution, items)

    # attachments
    for attachment in dataset_dict.get('attachments', []):
        # BNode: dataset_ref + url
        id_string = dataset_ref.n3() + attachment
        bnode_hash = hashlib.sha1(id_string.encode('utf-8'))
        distribution = BNode(bnode_hash.hexdigest())

        g.add((dataset_ref, DCAT.distribution, distribution))
        g.add((distribution, RDF.type, DCAT.Distribution))
        if license:
            # BNode: distribution + url
            id_string = distribution.n3() + license
            bnode_hash = hashlib.sha1(id_string.encode('utf-8'))
            l = BNode(bnode_hash.hexdigest())

            g.add((distribution, DCT.license, l))
            g.add((l, RDF.type, DCT.LicenseDocument))
            g.add((l, RDFS.label, Literal(license)))

        #  Simple values
        items = [
            ('title', DCT.title, None),
            ('mimetype', DCT.mediaType, None),
            ('format', DCT['format'], None),
        ]
        _add_triples_from_dict(g, attachment, distribution, items)

        # URL
        if attachment.get('id'):
            url = portal_url.rstrip('/') + '/api/datasets/1.0/' + identifier + '/attachments/' + attachment.get('id')
            g.add((distribution, DCT.accessURL, Literal(url)))
    return dataset_ref

class CKANConverter:
    def __init__(self, graph, portal_base_url):
        self.g = graph
        self.base_url = portal_base_url

    def graph_from_ckan(self, dataset_dict):
        dataset_ref = self.dataset_uri(dataset_dict)
        g = self.g

        for prefix, namespace in namespaces.iteritems():
            g.bind(prefix, namespace)

        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        # Basic fields
        items = [
            ('title', DCT.title, None),
            ('notes', DCT.description, None),
            ('url', DCAT.landingPage, None),
            ('identifier', DCT.identifier, ['guid', 'id']),
            ('version', OWL.versionInfo, ['dcat_version']),
            ('alternate_identifier', ADMS.identifier, None),
            ('version_notes', ADMS.versionNotes, None),
            ('frequency', DCT.accrualPeriodicity, ['frequency-of-update']),
            ('name', ADEQUATE.repositoryName, ['id']),
            ('categorization', ADEQUATE.category, None),
        ]
        _add_triples_from_dict(self.g, dataset_dict, dataset_ref, items)

        # Tags
        for tag in dataset_dict.get('tags', []):
            if isinstance(tag, dict):
                g.add((dataset_ref, DCAT.keyword, Literal(tag['name'])))
            elif isinstance(tag, basestring):
                g.add((dataset_ref, DCAT.keyword, Literal(tag)))

        # Dates
        items = [
            ('issued', DCT.issued, ['metadata_created']),
            ('modified', DCT.modified, ['metadata_modified']),
        ]
        _add_date_triples_from_dict(self.g, dataset_dict, dataset_ref, items)

        #  Lists
        items = [
            ('language', DCT.language, None),
            ('theme', DCAT.theme, None),
            ('conforms_to', DCAT.conformsTo, None),
        ]
        _add_list_triples_from_dict(self.g, dataset_dict, dataset_ref, items)

        # Contact details
        if any([
            self._get_dataset_value(dataset_dict, 'contact_uri'),
            self._get_dataset_value(dataset_dict, 'contact_name'),
            self._get_dataset_value(dataset_dict, 'contact_email'),
            self._get_dataset_value(dataset_dict, 'maintainer'),
            self._get_dataset_value(dataset_dict, 'maintainer_email'),
            self._get_dataset_value(dataset_dict, 'author'),
            self._get_dataset_value(dataset_dict, 'author_email'),
        ]):

            contact_uri = self._get_dataset_value(dataset_dict, 'contact_uri')
            if contact_uri:
                contact_details = URIRef(contact_uri)
            else:
                bnode_hash = hashlib.sha1(dataset_ref.n3() + DCAT.contactPoint.n3())
                contact_details = BNode(bnode_hash.hexdigest())

            g.add((contact_details, RDF.type, VCARD.Organization))
            g.add((dataset_ref, DCAT.contactPoint, contact_details))

            # contact-email was added as "most frequent extra key"
            items = [
                ('contact_name', VCARD.fn, ['maintainer', 'author']),
                ('contact_email', VCARD.hasEmail, ['maintainer_email',
                                                   'author_email',
                                                   'contact-email']),
            ]

            _add_triples_from_dict(self.g, dataset_dict, contact_details, items)

        # Publisher
        if any([
            self._get_dataset_value(dataset_dict, 'publisher_uri'),
            self._get_dataset_value(dataset_dict, 'publisher_name'),
            dataset_dict.get('organization'),
        ]):
            publisher_name = self._get_dataset_value(dataset_dict, 'publisher_name')
            if not publisher_name and dataset_dict.get('organization'):
                publisher_name = dataset_dict['organization']['title']

            publisher_uri = self.publisher_uri_from_dataset_dict(dataset_dict)
            if publisher_uri:
                publisher_details = URIRef(publisher_uri)
            else:
                # No organization nor publisher_uri
                id_string = dataset_ref.n3() + DCT.publisher.n3() + publisher_name
                bnode_hash = hashlib.sha1(id_string.encode('utf-8'))
                publisher_details = BNode(bnode_hash.hexdigest())

            g.add((publisher_details, RDF.type, FOAF.Organization))
            g.add((dataset_ref, DCT.publisher, publisher_details))

            g.add((publisher_details, FOAF.name, Literal(publisher_name)))
            # TODO: It would make sense to fallback these to organization
            # fields but they are not in the default schema and the
            # `organization` object in the dataset_dict does not include
            # custom fields
            items = [
                ('publisher_email', FOAF.mbox, None),
                ('publisher_url', FOAF.homepage, None),
                ('publisher_type', DCT.type, None),
            ]

            _add_triples_from_dict(self.g, dataset_dict, publisher_details, items)

        # Temporal
        start = self._get_dataset_value(dataset_dict, 'temporal_start', default='')
        end = self._get_dataset_value(dataset_dict, 'temporal_end', default='')
        if start or end:
            id_string = dataset_ref.n3() + DCT.temporal.n3() + start + end
            bnode_hash = hashlib.sha1(id_string.encode('utf-8'))
            temporal_extent = BNode(bnode_hash.hexdigest())

            g.add((temporal_extent, RDF.type, DCT.PeriodOfTime))
            if start:
                _add_date_triple(self.g, temporal_extent, SCHEMA.startDate, start)
            if end:
                _add_date_triple(self.g, temporal_extent, SCHEMA.endDate, end)
            g.add((dataset_ref, DCT.temporal, temporal_extent))

        # Spatial
        spatial_uri = self._get_dataset_value(dataset_dict, 'spatial_uri', default='')
        spatial_text = self._get_dataset_value(dataset_dict, 'spatial_text', default='')
        spatial_geom = self._get_dataset_value(dataset_dict, 'spatial', default='')

        if spatial_uri or spatial_text or spatial_geom:
            if spatial_uri:
                spatial_ref = URIRef(spatial_uri)
            else:
                id_string = dataset_ref.n3() + DCT.spatial.n3() + spatial_uri + spatial_text + spatial_geom
                bnode_hash = hashlib.sha1(id_string.encode('utf-8'))
                spatial_ref = BNode(bnode_hash.hexdigest())

            g.add((spatial_ref, RDF.type, DCT.Location))
            g.add((dataset_ref, DCT.spatial, spatial_ref))

            if spatial_text:
                g.add((spatial_ref, SKOS.prefLabel, Literal(spatial_text)))

            if spatial_geom:
                # GeoJSON
                g.add((spatial_ref,
                       LOCN.geometry,
                       Literal(spatial_geom, datatype=GEOJSON_IMT)))
                # WKT, because GeoDCAT-AP says so
                try:
                    g.add((spatial_ref,
                           LOCN.geometry,
                           Literal(wkt.dumps(json.loads(spatial_geom),
                                             decimals=4),
                                   datatype=GSP.wktLiteral)))
                except (TypeError, ValueError, InvalidGeoJSONException):
                    pass

        # License
        license_id = self._get_dataset_value(dataset_dict, 'license_id', default='')
        license_url = self._get_dataset_value(dataset_dict, 'license_url', default='')
        license_title = self._get_dataset_value(dataset_dict, 'license_title', default='')
        license = None
        if license_id or license_url or license_title:
            if license_url and bool(urlparse.urlparse(license_url).netloc):
                license = URIRef(license_url)
            else:
                id_string = dataset_ref.n3() + DCT.license.n3() + license_id + license_url + license_title
                bnode_hash = hashlib.sha1(id_string.encode('utf-8'))
                license = BNode(bnode_hash.hexdigest())
                # maybe a non-valid url
                if license_url:
                    g.add((license, RDFS.comment, Literal(license_url)))
            # l is a license document
            g.add((license, RDF.type, DCT.LicenseDocument))

            if license_title:
                g.add((license, RDFS.label, Literal(license_title)))
            if license_id:
                g.add((license, DCT.identifier, Literal(license_id)))

        # Resources
        for resource_dict in dataset_dict.get('resources', []):
            distribution = URIRef(self.resource_uri(resource_dict, dataset_dict.get('id')))

            g.add((dataset_ref, DCAT.distribution, distribution))
            g.add((distribution, RDF.type, DCAT.Distribution))

            # License
            if license:
                g.add((distribution, DCT.license, license))


            #  Simple values
            items = [
                ('name', DCT.title, None),
                ('description', DCT.description, None),
                ('status', ADMS.status, None),
                ('rights', DCT.rights, None),
                ('license', DCT.license, None),
            ]

            _add_triples_from_dict(self.g, resource_dict, distribution, items)

            # Format
            if '/' in resource_dict.get('format', ''):
                g.add((distribution, DCAT.mediaType,
                       Literal(resource_dict['format'])))
            else:
                if resource_dict.get('format'):
                    id_string = dataset_ref.n3() + DCT['format'].n3() + resource_dict['format']
                    bnode_hash = hashlib.sha1(id_string.encode('utf-8'))
                    f = BNode(bnode_hash.hexdigest())

                    g.add((f, RDF.type, DCT.MediaTypeOrExtent))
                    g.add((f, RDFS.label, Literal(resource_dict['format'])))
                    g.add((distribution, DCT['format'], f))
                    if resource_dict.get('mimetype'):
                        g.add((f, RDF.value, Literal(resource_dict['mimetype'])))

                if resource_dict.get('mimetype'):
                    g.add((distribution, DCAT.mediaType,
                           Literal(resource_dict['mimetype'])))

            # URL
            url = resource_dict.get('url')
            download_url = resource_dict.get('download_url')
            if download_url:
                download_url = download_url.strip()
                if is_valid_uri(download_url):
                    g.add((distribution, DCAT.downloadURL, URIRef(download_url)))
                else:
                    g.add((distribution, DCAT.downloadURL, Literal(download_url)))
            if (url and not download_url) or (url and url != download_url):
                url = url.strip()
                if is_valid_uri(url):
                    g.add((distribution, DCAT.accessURL, URIRef(url)))
                else:
                    g.add((distribution, DCAT.accessURL, Literal(url)))
            # Dates
            # metadata-date was added as "most frequent extra key"
            items = [
                ('issued', DCT.issued, ['created',
                                        'metadata-date']),
                ('modified', DCT.modified, ['last_modified']),
            ]

            _add_date_triples_from_dict(self.g, resource_dict, distribution, items)

            # Numbers
            if resource_dict.get('size'):
                try:
                    g.add((distribution, DCAT.byteSize,
                           Literal(float(resource_dict['size']),
                                   datatype=XSD.decimal)))
                except (ValueError, TypeError):
                    g.add((distribution, DCAT.byteSize,
                           Literal(resource_dict['size'])))
        return dataset_ref

    def _get_dataset_value(self, dataset_dict, key, default=None):
        '''
        Returns the value for the given key on a CKAN dict

        Check `_get_dict_value` for details
        '''
        return _get_dict_value(dataset_dict, key, default)

    def publisher_uri_from_dataset_dict(self, dataset_dict):
        '''
        Returns an URI for a dataset's publisher

        This will be used to uniquely reference the publisher on the RDF
        serializations.

        The value will be the first found of:

            1. The value of the `publisher_uri` field
            2. The value of an extra with key `publisher_uri`
            3. `catalog_uri()` + '/organization/' + `organization id` field

        Check the documentation for `catalog_uri()` for the recommended ways of
        setting it.

        Returns a string with the publisher URI, or None if no URI could be
        generated.
        '''

        uri = dataset_dict.get('pubisher_uri')
        if not uri:
            extras = dataset_dict.get('extras', [])
            if isinstance(extras, dict):
                uri = extras.get('publisher_uri')
            else:
                for extra in extras:
                    if extra['key'] == 'publisher_uri':
                        uri = extra['value']
                        break
        if not uri and dataset_dict.get('organization'):
            uri = '{0}/organization/{1}'.format(self.catalog_uri().rstrip('/'),
                                                dataset_dict['organization']['id'])

        return uri

    def catalog_uri(self):
        '''
        Returns an URI for the whole catalog

        This will be used to uniquely reference the CKAN instance on the RDF
        serializations and as a basis for eg datasets URIs (if not present on
        the metadata).

        Returns a string with the catalog URI.
        '''
        return self.base_url

    def resource_uri(self, resource_dict, dataset_id):
        '''
        Returns an URI for the resource

        This will be used to uniquely reference the resource on the RDF
        serializations.

        The value will be the first found of:

            1. The value of the `uri` field
            2. `catalog_uri()` + '/dataset/' + `package_id` + '/resource/' + `id` field

        Check the documentation for `catalog_uri()` for the recommended ways of
        setting it.

        Returns a string with the resource URI.
        '''

        uri = resource_dict.get('uri')
        if not uri:
            if not dataset_id:
                dataset_id = resource_dict.get('package_id')

            uri = '{0}/dataset/{1}/resource/{2}'.format(self.catalog_uri().rstrip('/'),
                                                        dataset_id,
                                                        resource_dict['id'])
        return uri

    def dataset_uri(self, dataset_dict):
        '''
        Returns an URI for the dataset

        This will be used to uniquely reference the dataset on the RDF
        serializations.

        The value will be the first found of:

            1. The value of the `uri` field
            2. The value of an extra with key `uri`
            3. `catalog_uri()` + '/dataset/' + `id` field

        Check the documentation for `catalog_uri()` for the recommended ways of
        setting it.

        Returns a string with the dataset URI.
        '''

        uri = dataset_dict.get('uri')
        if not uri:
            extras = dataset_dict.get('extras', [])
            if isinstance(extras, dict):
                for key, value in extras.items():
                    if key == 'uri':
                        uri = value
                        break
            elif isinstance(extras, list):
                for extra in extras:
                    if extra['key'] == 'uri':
                        uri = extra['value']
                        break
        if not uri and dataset_dict.get('id'):
            uri = '{0}/dataset/{1}'.format(self.catalog_uri().rstrip('/'),
                                           dataset_dict['id'])
        if not uri:
            uri = '{0}/dataset/{1}'.format(self.catalog_uri().rstrip('/'),
                                           str(uuid.uuid4()))
            # TODO log.warning('Using a random id for dataset URI')

        uri = URIRef(uri)
        return uri


def _add_triples_from_dict(graph, _dict, subject, items,
                           list_value=False,
                           date_value=False):
    for item in items:
        key, predicate, fallbacks = item
        _add_triple_from_dict(graph, _dict, subject, predicate, key,
                                   fallbacks=fallbacks,
                                   list_value=list_value,
                                   date_value=date_value)

def _add_date_triples_from_dict(graph, _dict, subject, items):
    _add_triples_from_dict(graph, _dict, subject, items,
                                date_value=True)

def _add_list_triples_from_dict(graph, _dict, subject, items):
    _add_triples_from_dict(graph, _dict, subject, items,
                                list_value=True)

def _add_triple_from_dict(graph, _dict, subject, predicate, key,
                          fallbacks=None,
                          list_value=False,
                          date_value=False):
    '''
    Adds a new triple to the graph with the provided parameters

    The subject and predicate of the triple are passed as the relevant
    RDFLib objects (URIRef or BNode). The object is always a literal value,
    which is extracted from the dict using the provided key (see
    `_get_dict_value`). If the value for the key is not found, then
    additional fallback keys are checked.

    If `list_value` or `date_value` are True, then the value is treated as
    a list or a date respectively (see `_add_list_triple` and
    `_add_date_triple` for details.
    '''
    value = _get_dict_value(_dict, key)
    if not value and fallbacks:
        for fallback in fallbacks:
            value = _get_dict_value(_dict, fallback)
            if value:
                break

    if value and list_value:
        _add_list_triple(graph, subject, predicate, value)
    elif value and date_value:
        _add_date_triple(graph, subject, predicate, value)
    elif value:
        # Normal text value
        if '"label"' in value:
            try:
                d=json.loads(value)
                value=d['label']
            except:
                value=value

        graph.add((subject, predicate, Literal(value)))

def _get_dict_value(_dict, key, default=None):
    '''
    Returns the value for the given key on a CKAN dict

    By default a key on the root level is checked. If not found, extras
    are checked, both with the key provided and with `dcat_` prepended to
    support legacy fields.

    If not found, returns the default value, which defaults to None
    '''

    if key in _dict:
        return _dict[key]

    extras = _dict.get('extras', [])
    if isinstance(extras, dict):
        for k in [key, 'dcat_' + key]:
            if k in extras:
                return extras[k]
    else:
        for extra in extras:
            if extra['key'] == key or extra['key'] == 'dcat_' + key:
                return extra['value']
    return default


def _add_list_triple(graph, subject, predicate, value):
    '''
    Adds as many triples to the graph as values

    Values are literal strings, if `value` is a list, one for each
    item. If `value` is a string there is an attempt to split it using
    commas, to support legacy fields.
    '''
    items = []
    # List of values
    if isinstance(value, list):
        items = value
    elif isinstance(value, basestring):
        try:
            # JSON list
            items = json.loads(value)
        except ValueError:
            if ',' in value:
                # Comma-separated list
                items = value.split(',')
            else:
                # Normal text value
                items = [value]

    for item in items:
        graph.add((subject, predicate, Literal(item)))


def _add_date_triple(graph, subject, predicate, value):
    '''
    Adds a new triple with a date object

    Dates are parsed using dateutil, and if the date obtained is correct,
    added to the graph as an XSD.dateTime value.

    If there are parsing errors, the literal string value is added.
    '''
    if not value:
        return
    try:
        default_datetime = datetime.datetime(1, 1, 1, 0, 0, 0)
        _date = parse_date(value, default=default_datetime)

        graph.add((subject, predicate, Literal(_date.isoformat(),
                                                datatype=XSD.dateTime)))
    except ValueError:
        graph.add((subject, predicate, Literal(value)))