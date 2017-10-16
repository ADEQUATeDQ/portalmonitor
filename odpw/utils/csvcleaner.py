from StringIO import StringIO

import structlog
from pyyacp import yacp
import codecs
import os
import csv
import rdflib
from rdflib import URIRef, BNode, Literal
from rdflib.namespace import Namespace, RDF, RDFS
from odpw.quality.dqv_export import DCAT, PROV
from odpw.services import stream_csv
from odpw.utils.error_handling import ErrorHandler
from odpw.utils.utils_snapshot import tofirstdayinisoweek, toLastdayinisoweek, getCurrentSnapshot

AD_AGENT = URIRef("http://www.adequate.at/")
AD = Namespace("http://www.adequate.at/ns/")


log =structlog.get_logger()


def adequate_prov(graph, snapshot):
    ad_activity = URIRef("http://www.adequate.at/csvprofiler/" + str(snapshot))
    graph.add((ad_activity, RDF.type, PROV.Activity))
    graph.add((ad_activity, PROV.startedAtTime, Literal(tofirstdayinisoweek(snapshot))))
    graph.add((ad_activity, PROV.endedAtTime, Literal(toLastdayinisoweek(snapshot))))
    graph.add((ad_activity, PROV.wasAssociatedWith, AD_AGENT))
    return ad_activity


def csv_clean(filename, git_url, orig_url, metadata, stream_orig=True):
    # TODO read csv files in dir, run pyyacp and and track modifications, read jsonld, add new resource with description and modifications
    out_encoding = 'utf-8'
    out_delimiter = ','

    reader = yacp.YACParser(filename=filename)
    deli = reader.meta['delimiter']
    encoding = reader.meta['encoding']
    descriptionLines = reader.descriptionLines
    header_line = reader.header_line


    f_name = os.path.basename(filename)

    cleaned_path = os.path.join(os.path.dirname(filename), '..', 'cleaned')
    if not os.path.exists(cleaned_path):
        os.mkdir(cleaned_path)
    cleaned_content = reader.generate(delimiter=out_delimiter, comments=False)

    with codecs.open(os.path.join(cleaned_path, f_name), 'w', out_encoding) as out_f:
        out_f.write(cleaned_content.decode(out_encoding))

    g = rdflib.Graph()
    g.parse(metadata, format="json-ld")

    snapshot = getCurrentSnapshot()
    activity = adequate_prov(g, snapshot)
    if stream_orig:
        try:
            # add csvw info to orig resource
            stream_csv.addMetadata(orig_url, snapshot, g, csvw_activity=activity)
        except Exception as e:
            ErrorHandler.handleError(log, "GetCSVWMetadata", exception=e, url=orig_url, snapshot=snapshot,
                                     exc_info=True)

    # add new resource
    dataset_ref = g.value(predicate=RDF.type, object=DCAT.Dataset)
    git_res_page = git_url + 'blob/master/resources/' + f_name
    git_res_raw = git_url + 'raw/master/resources/' + f_name

    #orig_dist = g.subjects(predicate=DCAT.accessURL, object=URIRef(orig_url))

    distribution = URIRef(git_res_page)
    access_url = URIRef(git_res_raw)

    g.add((dataset_ref, DCAT.distribution, distribution))
    g.add((distribution, RDF.type, DCAT.Distribution))
    g.add((distribution, DCAT.accessURL, access_url))

    # prov information
    g.add((access_url, RDF.type, PROV.Entity))
    g.add((access_url, PROV.wasDerivedFrom, URIRef(orig_url)))
    g.add((access_url, PROV.wasGeneratedBy, activity))
    g.add((activity, PROV.generated, access_url))

    # add CSV modifications to metadata
    if not header_line:
        g.add((activity, AD.csvCleanModification, AD.GenericHeader))
    if deli != out_delimiter:
        g.add((activity, AD.csvCleanModification, AD.DefaultDelimiter))
    if encoding != out_encoding:
        g.add((activity, AD.csvCleanModification, AD.Utf8Encoding))
    if descriptionLines:
        g.add((activity, AD.csvCleanModification, AD.DropCommentLines))
        # add comment lines metadata
        for l in descriptionLines:
            out = StringIO()
            w = csv.writer(out)
            w.writerow([v.encode(out_encoding) for v in l])
            g.add((distribution, RDFS.comment, Literal(out.getvalue())))

    g.serialize(destination=metadata, format='json-ld')
