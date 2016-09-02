# -*- coding: utf-8 -*-

# SOURCE:
# https://raw.githubusercontent.com/opendatamonitor/odm.restapi/master/odmapi/def_formatLists.py
# 2015-10-01

def get_machine_readable():
    machine_readable = [
         'cdf',
         'csv',
         'csv.zip',
         'esri shapefile',
         'geojson',
         'iati',
         'ical',
         'ics',
         'json',
         'kml',
         'kmz',
         'netcdf',
         'nt',
         'ods',
         'psv',
         'psv.zip',
         'rdf',
         'rdfa',
         'rss',
         'shapefile',
         'shp',
         'shp.zip',
         'sparql',
         'sparql web form',
         'tsv',
         'ttl',
         'wms',
         'xlb',
         'xls',
         'xls.zip',
         'xlsx',
         'xml',
         'xml.zip',
     ]

    return machine_readable

def get_non_proprietary():
    non_propr = [
         'ascii',
         'audio/mpeg',
         'bmp',
         'cdf',
         'csv',
         'csv.zip',
         'dbf',
         'geojson',
         'geotiff',
         'gzip',
         'html',
         'iati',
         'ical',
         'ics',
         'jpeg 2000',
         'json',
         'kml',
         'kmz',
         'mpeg',
         'netcdf',
         'nt',
         'ods',
         'pdf',
         'pdf/a',
         'png',
         'psv',
         'psv.zip',
         'rdf',
         'rdfa',
         'rss',
         'rtf',
         'sparql',
         'sparql web form',
         'tar',
         'tiff',
         'tsv',
         'ttl',
         'txt',
         'wms',
         'xml',
         'xml.zip',
         'zip',
         ]

    return non_propr

def get_open_licenses():
    open_licenses=[
         u'CC0',u'CC0-1.0',
         u'ODC-PDDL',u'Open Data Commons Public Domain Dedication and Licence',u'PDDL',
         u'CC BY',u'CC BY-2.5',u'CC BY-3.0',u'CC BY-3.0 AT',u'CC BY-3.0 DE',
         u'CC BY-3.0 ES',u'CC BY-3.0 FI',u'CC BY-3.0 IT',u'CC BY-4.0',
         u'ODC-BY',
         u'CC BY-SA',u'CC BY-SA-1.0 FI',u'CC BY-SA-3.0 AT',u'CC BY-SA-3.0 CH',
         u'CC BY-SA-3.0 ES',u'CC-BY-SA-4.0',
         u'ODC-ODbL',u'ODbL',
         u'Free Art License',u'FAL',
         u'GFDL',u'GNU FDL',
         u'GNU GPL-2.0',u'GNU GPL-3.0',
         u'OGL',u'OGL-UK-2.0',u'OGL-UK-3.0 ',
         u'Open Government Licence - Canada 2.0',u'Open Government Licence Canada 2.0',
         u'OGL-Canada-2.0',
         u'MirOS License',
         u'Talis Community License',
         u'Against DRM',
         u'DL-DE-BY-2.0',u'Data licence Germany – attribution – version 2.0',u'Data licence Germany –    Zero – version 2.0',
         u'Design Science License',u'EFF Open Audio License',
         u'SPL',
         u'W3C license'
     ]

    return open_licenses
