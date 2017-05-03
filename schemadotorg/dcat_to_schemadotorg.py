import rdflib
from rdflib import URIRef, BNode, Literal
from rdflib.namespace import Namespace, RDF, RDFS, DCTERMS, XSD

from odpw.core import dataset_converter

import json

DCAT = Namespace("http://www.w3.org/ns/dcat#")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")

# https://www.w3.org/2015/spatial/wiki/ISO_19115_-_DCAT_-_Schema.org_mapping

def resp_party(g, doc, orga):
    if isinstance(orga, URIRef) and '@id' not in doc:
        doc['@id'] = str(orga)
    for p, o in g.predicate_objects(orga):
        if 'name' not in doc:
            if p == FOAF.name:
                doc['name'] = str(o)
            elif p == VCARD.fn:
                doc['name'] = str(o)
            elif p == VCARD['organization-name']:
                doc['name'] = str(o)
        if 'email' not in doc:
            if p == FOAF.mbox:
                doc['email'] = str(o)
            elif p == VCARD.hasEmail:
                doc['email'] = str(o)
        if 'url' not in doc:
            if p == FOAF.homepage:
                doc['url'] = str(o)
            elif p == VCARD.hasURL:
                doc['url'] = str(o)
    return doc


def convert(portal, data):
    g = rdflib.Graph()
    # write dcat dataset into graph
    dataset_converter.dict_to_dcat(data, portal, graph=g)

    ds_id = g.value(predicate=RDF.type, object=DCAT.Dataset)
    doc = {
        "@context": "http://schema.org",
        "@type": "Dataset",
        "@id": str(ds_id),
        "catalog": {
            "@type": "DataCatalog",
            "@id": portal.uri,
            "url": portal.uri,
            "spatialCoverage": portal.iso,
            "description": "Underlying software: " + portal.software
        }
    }
    # organization
    if (ds_id, DCTERMS.publisher, None) in g:
        pub = {
            "@type": "Organization"
        }
        orga = g.value(ds_id, DCTERMS.publisher)
        resp_party(g, pub, orga)
        # contact point
        if (ds_id, DCAT.contactPoint, None) in g:
            orga = g.value(ds_id, DCAT.contactPoint)
            resp_party(g, pub, orga)
        doc['publisher'] = pub

    # general fields
    if (ds_id, DCTERMS.title, None) in g:
        doc["name"] = str(g.value(ds_id, DCTERMS.title))
    if (ds_id, DCTERMS.description, None) in g:
        doc["description"] = str(g.value(ds_id, DCTERMS.description))
    if (ds_id, DCAT.landingPage, None) in g:
        doc["url"] = str(g.value(ds_id, DCTERMS.landingPage))
    if (ds_id, DCTERMS.spatial, None) in g:
        doc["spatialCoverage"] = str(g.value(ds_id, DCTERMS.spatial))
    if (ds_id, DCTERMS.temporal, None) in g:
        doc["datasetTimeInterval"] = str(g.value(ds_id, DCTERMS.temporal))
    if (ds_id, DCAT.theme, None) in g:
        doc["about"] = str(g.value(ds_id, DCAT.theme))
    if (ds_id, DCTERMS.modified, None) in g:
        doc["dateModified"] = str(g.value(ds_id, DCTERMS.modified))
    if (ds_id, DCTERMS.issued, None) in g:
        doc["datePublished"] = str(g.value(ds_id, DCTERMS.issued))
    if (ds_id, DCTERMS.language, None) in g:
        doc["inLanguage"] = str(g.value(ds_id, DCTERMS.language))

    if (ds_id, DCAT.keyword, None) in g:
        doc["keywords"] = []
        for keyword in g.objects(ds_id, DCAT.keyword):
            doc["keywords"].append(str(keyword))

    doc["distribution"] = []
    for dist_id in g.objects(ds_id, DCAT.distribution):
        dist = {
            "@type": "DataDownload",
            "@id": str(dist_id)
        }

        if (dist_id, DCTERMS.title, None) in g:
            dist["name"] = str(g.value(dist_id, DCTERMS.title))
        if (dist_id, DCTERMS.description, None) in g:
            dist["description"] = str(g.value(dist_id, DCTERMS.description))
        if (dist_id, DCTERMS.modified, None) in g:
            dist["dateModified"] = str(g.value(dist_id, DCTERMS.modified))
        if (dist_id, DCTERMS.issued, None) in g:
            dist["datePublished"] = str(g.value(dist_id, DCTERMS.issued))
        if (dist_id, DCTERMS['format'], None) in g:
            dist["encodingFormat"] = str(g.value(dist_id, DCTERMS['format']))
        if (dist_id, DCAT.byteSize, None) in g:
            dist["contentSize"] = str(g.value(dist_id, DCAT.byteSize))
        if (dist_id, DCAT.mediaType, None) in g:
            dist["fileFormat"] = str(g.value(dist_id, DCAT.mediaType))

        if (dist_id, DCAT.accessURL, None) in g:
            dist["contentUrl"] = str(g.value(dist_id, DCAT.accessURL))
        elif (dist_id, DCAT.downloadURL, None) in g:
            dist["contentUrl"] = str(g.value(dist_id, DCAT.downloadURL))

        if (dist_id, DCTERMS.license, None) in g:
            l = g.value(dist_id, DCTERMS.license)
            if isinstance(l, BNode):
                # look for description
                if (l, RDFS.label, None) in g:
                    dist["license"] = str(g.value(l, RDFS.label))
                elif (l, DCTERMS.identifier, None) in g:
                    dist["license"] = str(g.value(l, DCTERMS.identifier))
            else:
                dist["license"] = str(l)
        doc["distribution"].append(dist)

    return doc