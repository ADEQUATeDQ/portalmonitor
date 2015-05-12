import requests
import logging



refs = {}
refs['Dataset Register'] = '/rest/dataset'
refs['Group Register'] = '/rest/group'
refs['Tag Register'] = '/rest/tag'
refs['Rating Register'] = '/rest/rating'
refs['Revision Register'] = '/rest/revision'
refs['License List'] = '/rest/licenses'
refs['Status'] = "/util/status"
refs['Dataset Search'] = '/search/dataset'
refs['Resource Search'] = '/search/resource'
refs['Revision Search'] = '/search/revision'
refs['Tag Counts'] = '/tag_counts'



def openURL( url , path):
    global logger
    logger = logging.getLogger(__name__)

    resp = requests.get(url+path, timeout=60)
    logger.debug("GET: %s %s", url+path,resp)
    return resp

def package_entity( url , entity):
    return openURL(url, refs['Dataset Register']+"/"+entity)


def package_get( url ):
    return openURL(url, refs['Dataset Register'])

def group_get( url ):
    return openURL(url, refs['Group Register'])

def status( url ):
    return openURL( url, refs['Status'])

#def modifiedSince( date):
#    http://it.ckan.net/api/search/revision?since_time=2011-04-17

def license_get( url):
    return openURL(url, refs['License List'])


def package_modifiedSince(url):
    data = openURL(url, refs['Revision Search'])
    return data

def tags_get(url):
    data = openURL(url, refs['Tag Register'])
    return data

def tags_counts(url):
    data = openURL(url, refs['Tag Counts'])
    return data




