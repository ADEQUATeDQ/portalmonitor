import hashlib
from odpw.core import dataset_converter
import rdflib
from rdflib import URIRef, BNode, Literal
from rdflib.namespace import Namespace, RDF, SKOS, RDFS, XSD, FOAF
from odpw.utils import utils_snapshot


DAQ = Namespace("http://purl.org/eis/vocab/daq#")
DCAT = Namespace("http://www.w3.org/ns/dcat#")
DQV = Namespace("http://www.w3.org/ns/dqv#")
DUV = Namespace("http://www.w3.org/ns/duv#")
OA = Namespace("http://www.w3.org/ns/oa#")
PROV = Namespace("http://www.w3.org/ns/prov#")
SDMX = Namespace("http://purl.org/linked-data/sdmx/2009/attribute#")

PWQ = Namespace("http://data.wu.ac.at/portalwatch/quality#")

PW_AGENT = URIRef("http://data.wu.ac.at/portalwatch")

def general_prov(graph):
    graph.add((PW_AGENT, RDF.type, PROV.SoftwareAgent))
    graph.add((PW_AGENT, RDFS.label, Literal("Open Data Portal Watch")))
    graph.add((PW_AGENT, FOAF.mbox, URIRef("mailto:contact@data.wu.ac.at")))
    graph.add((PW_AGENT, FOAF.homePage, PW_AGENT))


def fetch_prov(snapshot, portalid, start_time, end_time, graph):
    pw_activity = URIRef("http://data.wu.ac.at/portalwatch/portal/" + portalid + '/' + str(snapshot))
    graph.add((pw_activity, RDF.type, PROV.Activity))
    graph.add((pw_activity, PROV.startedAtTime, Literal(start_time)))
    graph.add((pw_activity, PROV.endedAtTime, Literal(end_time)))
    graph.add((pw_activity, PROV.wasAssociatedWith, PW_AGENT))
    return pw_activity


def quality_prov(entity, dataset, gen_time, fetch_activity, graph):
    graph.add((entity, RDF.type, PROV.Entity))

    graph.add((entity, PROV.wasDerivedFrom, dataset))
    #graph.add((qual_measure, PROV.wasGeneratedAt, Literal(gen_time)))
    #graph.add((qual_measure, PROV.generatedAtTime, Literal(gen_time)))

    graph.add((entity, PROV.wasGeneratedBy, fetch_activity))
    graph.add((fetch_activity, PROV.generated, entity))



def add_dimensions_and_metrics(g):
    ####################################################################
    ex = PWQ.Existence
    g.add((ex, RDF.type, DQV.Dimension))
    g.add((ex, SKOS.prefLabel, Literal("Existence")))
    g.add((ex, SKOS.definition, Literal("Existence of important information (i.e. exist certain metadata keys)")))

    date = PWQ.Date
    g.add((date, RDF.type, DQV.Metric))
    g.add((date, SKOS.prefLabel, Literal("Date")))
    g.add((date, SKOS.definition, Literal("Does the meta data contain information about creation and modification date of metadata and resources respectively?")))
    g.add((date, RDFS.comment, Literal("Some of the creation and modification date fields for the dataset and resources are empty")))
    g.add((date, DQV.expectedDataType, XSD.double))
    g.add((date, DQV.inDimension, ex))

    rights = PWQ.Rights
    g.add((rights, RDF.type, DQV.Metric))
    g.add((rights, SKOS.prefLabel, Literal("Rights")))
    g.add((rights, SKOS.definition, Literal("Does the meta data contain information about the license of the dataset or resource?")))
    g.add((rights, RDFS.comment, Literal("The dataset has no license information")))
    g.add((rights, DQV.expectedDataType, XSD.double))
    g.add((rights, DQV.inDimension, ex))

    x = PWQ.Preservation
    g.add((x, RDF.type, DQV.Metric))
    g.add((x, SKOS.prefLabel, Literal("Preservation")))
    g.add((x, SKOS.definition, Literal("Does the meta data contain information about format, size or update frequency of the resources?")))
    g.add((x, RDFS.comment, Literal("Information (size, format, mimetype, ..) for preserving/archiving the dataset resource are missing")))
    g.add((x, DQV.expectedDataType, XSD.double))
    g.add((x, DQV.inDimension, ex))

    x = PWQ.Access
    g.add((x, RDF.type, DQV.Metric))
    g.add((x, SKOS.prefLabel, Literal("Access")))
    g.add((x, SKOS.definition, Literal("Does the meta data contain access information for the resources?")))
    g.add((x, RDFS.comment, Literal("Some of the resources do not have an access URL")))
    g.add((x, DQV.expectedDataType, XSD.double))
    g.add((x, DQV.inDimension, ex))

    x = PWQ.Discovery
    g.add((x, RDF.type, DQV.Metric))
    g.add((x, SKOS.prefLabel, Literal("Discovery")))
    g.add((x, SKOS.definition, Literal("Does the meta data contain information that can help to discover/search datasets?")))
    g.add((x, RDFS.comment, Literal("Some of the title, description and keyword fields are empty")))
    g.add((x, DQV.expectedDataType, XSD.double))
    g.add((x, DQV.inDimension, ex))

    x = PWQ.Contact
    g.add((x, RDF.type, DQV.Metric))
    g.add((x, SKOS.prefLabel, Literal("Contact")))
    g.add((x, SKOS.definition, Literal("Does the meta data contain information to contact the data provider or publisher?")))
    g.add((x, RDFS.comment, Literal("Contact information is missing")))
    g.add((x, DQV.expectedDataType, XSD.double))
    g.add((x, DQV.inDimension, ex))

    ####################################################################
    co = PWQ.Conformance
    g.add((co, RDF.type, DQV.Dimension))
    g.add((co, SKOS.prefLabel, Literal("Conformance")))
    g.add((co, SKOS.definition, Literal("Does information adhere to a certain format if it exist?")))

    x = PWQ.ContactURL
    g.add((x, RDF.type, DQV.Metric))
    g.add((x, SKOS.prefLabel, Literal("ContactURL")))
    g.add((x, SKOS.definition, Literal("Are the available values of contact properties valid HTTP URLs?")))
    g.add((x, RDFS.comment, Literal("The publisher or contact URL is not a syntactically valid URI")))
    g.add((x, DQV.expectedDataType, XSD.double))
    g.add((x, DQV.inDimension, co))

    x = PWQ.DateFormat
    g.add((x, RDF.type, DQV.Metric))
    g.add((x, SKOS.prefLabel, Literal("DateFormat")))
    g.add((x, SKOS.definition, Literal("Is date information specified in a valid date format?")))
    g.add((x, RDFS.comment, Literal("Some of the creation and modification dates are not in a valid date format")))
    g.add((x, DQV.expectedDataType, XSD.double))
    g.add((x, DQV.inDimension, co))

    x = PWQ.FileFormat
    g.add((x, RDF.type, DQV.Metric))
    g.add((x, SKOS.prefLabel, Literal("FileFormat")))
    g.add((x, SKOS.definition, Literal("Is the specified file format or media type registered by IANA?")))
    g.add((x, RDFS.comment, Literal("Some of the specified mime types and file format are not registered with IANA (iana.org/)")))
    g.add((x, DQV.expectedDataType, XSD.double))
    g.add((x, DQV.inDimension, co))

    x = PWQ.ContactEmail
    g.add((x, RDF.type, DQV.Metric))
    g.add((x, SKOS.prefLabel, Literal("ContactEmail")))
    g.add((x, SKOS.definition, Literal("Are the available values of contact properties valid emails?")))
    g.add((date, RDFS.comment, Literal("The publisher or contact Email is not a syntactically valid Email")))
    g.add((x, DQV.expectedDataType, XSD.double))
    g.add((x, DQV.inDimension, co))

    x = PWQ.License
    g.add((x, RDF.type, DQV.Metric))
    g.add((x, SKOS.prefLabel, Literal("License")))
    g.add((x, SKOS.definition, Literal("Can the license be mapped to the list of licenses reviewed by opendefinition.org?")))
    g.add((x, RDFS.comment, Literal("The specified license could not mapped to the list provided by opendefinition.org")))
    g.add((x, DQV.expectedDataType, XSD.double))
    g.add((x, DQV.inDimension, co))

    x = PWQ.AccessURL
    g.add((x, RDF.type, DQV.Metric))
    g.add((x, SKOS.prefLabel, Literal("AccessURL")))
    g.add((x, SKOS.definition, Literal("Are the available values of access properties valid HTTP URLs?")))
    g.add((x, RDFS.comment, Literal("The download or access URL is not a syntactically valid URL")))
    g.add((x, DQV.expectedDataType, XSD.double))
    g.add((x, DQV.inDimension, co))

    #####################################################################
    od = PWQ.OpenData
    g.add((od, RDF.type, DQV.Dimension))
    g.add((od, SKOS.prefLabel, Literal("Open Data")))
    g.add((od, SKOS.definition, Literal("Is the specified format and license information suitable to classify a dataset as open?")))

    x = PWQ.OpenFormat
    g.add((x, RDF.type, DQV.Metric))
    g.add((x, SKOS.prefLabel, Literal("Format Openness")))
    g.add((x, SKOS.definition, Literal("Is the file format based on an open standard?")))
    g.add((x, RDFS.comment, Literal("Some of the specified formats are not considered open")))
    g.add((x, DQV.expectedDataType, XSD.double))
    g.add((x, DQV.inDimension, od))

    x = PWQ.MachineRead
    g.add((x, RDF.type, DQV.Metric))
    g.add((x, SKOS.prefLabel, Literal("Format machine readability")))
    g.add((x, SKOS.definition, Literal("Can the file format be considered as machine readable?")))
    g.add((x, RDFS.comment, Literal("Some of the specified formats are not considered as machine readable")))
    g.add((x, DQV.expectedDataType, XSD.double))
    g.add((x, DQV.inDimension, od))

    x = PWQ.OpenLicense
    g.add((x, RDF.type, DQV.Metric))
    g.add((x, SKOS.prefLabel, Literal("License Openneness")))
    g.add((x, SKOS.definition, Literal("Is the used license conform to the open definition?")))
    g.add((x, RDFS.comment, Literal("The specified license is not considered to be open by the opendefinition.org")))
    g.add((x, DQV.expectedDataType, XSD.double))
    g.add((x, DQV.inDimension, od))


def get_measures_and_dataset(portal, dataset, datasetquality, graph=None, portal_uri=None, fetch_activity=None):
    if graph == None:
        graph = rdflib.Graph()
    # write dcat dataset into graph
    dataset_ref = dataset_converter.add_dcat_to_graph(dataset.data.raw, portal, graph=graph, portal_uri=portal_uri)
    if dataset_ref and fetch_activity:
        # add prov information to dataset_ref
        graph.add((dataset_ref, PROV.wasGeneratedBy, fetch_activity))
        graph.add((fetch_activity, PROV.generated, dataset_ref))
        graph.add((dataset_ref, RDF.type, PROV.Entity))
    if dataset_ref:
        dataset_quality_to_dqv(graph, dataset_ref, datasetquality, dataset.snapshot, fetch_activity)
    return graph


def _get_measures_for_dataset(portal, dataset, datasetquality):
    graph = rdflib.Graph()
    # write dcat dataset into graph
    dataset_converter.dict_to_dcat(dataset.data.raw, portal, graph=graph)
    measures_g = rdflib.Graph()
    ds_id = graph.value(predicate=RDF.type, object=DCAT.Dataset)
    dataset_quality_to_dqv(measures_g, ds_id, datasetquality, dataset.snapshot)
    return measures_g, ds_id

def get_measures_for_dataset(portal, dataset, datasetquality):
    measures_g, datasetref = _get_measures_for_dataset(portal, dataset, datasetquality)
    return measures_g


def dataset_quality_to_dqv(graph, ds_id, datasetquality, snapshot, fetch_activity=None):
    sn_time = utils_snapshot.tofirstdayinisoweek(snapshot)

    # BNodes: ds_id + snapshot + metric + value
    # add quality metrics to graph
    # TODO should we use portalwatch URI?
    for metric, value in [(PWQ.Date, datasetquality.exda), (PWQ.Rights, datasetquality.exri), (PWQ.Preservation, datasetquality.expr),
                          (PWQ.Access, datasetquality.exac), (PWQ.Discovery, datasetquality.exdi), (PWQ.Contact, datasetquality.exco),
                          (PWQ.ContactURL, datasetquality.cocu), (PWQ.DateFormat, datasetquality.coda), (PWQ.FileFormat, datasetquality.cofo),
                          (PWQ.ContactEmail, datasetquality.coce), (PWQ.License, datasetquality.coli), (PWQ.AccessURL, datasetquality.coac),
                          (PWQ.OpenFormat, datasetquality.opfo), (PWQ.MachineRead, datasetquality.opma), (PWQ.OpenLicense, datasetquality.opli)]:
        # add unique BNodes
        bnode_hash = hashlib.sha1(ds_id.n3() + str(snapshot) + metric.n3() + str(value))
        m = BNode(bnode_hash.hexdigest())

        graph.add((m, DQV.isMeasurementOf, metric))
        graph.add((m, DQV.value, Literal(value)))

        # add additional triples
        graph.add((ds_id, DQV.hasQualityMeasurement, m))
        graph.add((m, RDF.type, DQV.QualityMeasurement))
        graph.add((m, DQV.computedOn, ds_id))
        if fetch_activity:
            # add prov to each measure
            quality_prov(m, ds_id, sn_time, fetch_activity, graph)
