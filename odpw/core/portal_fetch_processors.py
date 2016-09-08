from ast import literal_eval
import random
import urlparse
import ckanapi
import time
import requests
from pprint import pformat

#from odpw.db.models import Dataset


import structlog

from odpw.utils import extras_to_dicts, extras_to_dict
from odpw.utils.error_handling import ErrorHandler, TimeoutError, getExceptionCode, getExceptionString
from odpw.utils.timing import progressIndicator, Timer

log = structlog.get_logger()




def getPortalProcessor(P):
    if P.software == 'CKAN':
        return CKAN()
    elif P.software == 'Socrata':
        return Socrata()
    elif P.software == 'OpenDataSoft':
        return OpenDataSoft()
    else:
        raise NotImplementedError(P.software + ' is not implemented')

class PortalProcessor:
    def generateFetchDatasetIter(self, Portal, sn):
        raise NotImplementedError("Should have implemented this")

class CKAN(PortalProcessor):
    def _waiting_time(self, attempt):
        if attempt == 1:
            return 3
        else:
            return attempt*attempt*5

    def _get_datasets(self, api, timeout_attempts, rows, start, portal_id):
        #using timeout_attempts attempts

        for attempt in xrange(timeout_attempts):
            time.sleep(self._waiting_time(attempt))
            try:
                response = api.action.package_search(rows=rows, start=start)
                return response
            except ckanapi.errors.CKANAPIError as e:
                err = literal_eval(e.extra_msg)
                if 500 <= err[1] < 600:
                    rows =rows/3 if rows>=3 else rows
                    log.warn("CKANPackageSearchFetch", pid=portal_id, error='Internal Server Error. Retrying after waiting time.', errorCode=str(err[1]), attempt=attempt, waiting=self._waiting_time(attempt), rows=rows)
                else:
                    raise e
        raise e

    def generateFetchDatasetIter(self, Portal, sn, timeout_attempts=5, timeout=24*60*60):
        starttime=time.time()
        api = ckanapi.RemoteCKAN(Portal.apiuri, get_only=True)
        start=0
        rows=1000

        p_count=0
        p_steps=1
        total=0
        processed_ids=set([])
        processed_names=set([])

        tstart=time.time()
        try:
            response = api.action.package_search(rows=0)
            total = response["count"]
            p_steps=total/10
            if p_steps ==0:
                p_steps=1

            while True:
                response = self._get_datasets(api, timeout_attempts, rows, start, Portal.id)

                #print Portal.apiurl, start, rows, len(processed)
                datasets = response["results"] if response else None
                if datasets:
                    rows = len(datasets)
                    start+=rows
                    for datasetJSON in datasets:
                        datasetID = datasetJSON['id']
                        try:
                            if datasetID not in processed_ids:
                                data = datasetJSON
                                extras_to_dicts(data)

                                d = Dataset(snapshot=sn, portalID=Portal.id, did=datasetID, data=data, status=200, software=Portal.software)
                                processed_ids.add(d.id)
                                processed_names.add(datasetJSON['name'])

                                p_count+=1
                                if p_count%p_steps ==0:
                                    progressIndicator(p_count, total, label=Portal.id,elapsed=time.time()-tstart)
                                    log.info("ProgressDSFetchBatch", pid=Portal.id, processed=len(processed_ids))

                                now = time.time()
                                if now-starttime>timeout:
                                    raise TimeoutError("Timeout of "+Portal.id+" and "+str(timeout)+" seconds", timeout)
                                yield d

                        except Exception as e:
                            ErrorHandler.handleError(log,"CKANDSFetchDatasetBatchError", pid=Portal.id, did=datasetID, exception=e, exc_info=True)

                    rows = min([int(rows*1.2),1000])
                else:
                    break
            progressIndicator(p_count, total, label=Portal.id+"_batch",elapsed=time.time()-tstart)
        except TimeoutError as e:
            raise e
        except Exception as e:
            ErrorHandler.handleError(log,"CKANDSFetchBatchError", pid=Portal.id, exception=e, exc_info=True)
        try:
            package_list, status = getPackageList(Portal.apiuri)
            if total >0 and len(package_list) !=total:
                log.info("PackageList_COUNT", total=total, pid=Portal.id, pl=len(package_list))
            #len(package_list)
            tt=len(package_list)
            p_steps=tt/100
            if p_steps == 0:
                p_steps=1
            p_count=0
            # TODO parameter:
            NOT_SUPPORTED_PENALITY = 100
            TIMEOUT_PENALITY = 100
            not_supported_count = 0
            timeout_counts = 0
            for entity in package_list:
                #WAIT between two consecutive GET requests

                if entity not in processed_ids and entity not in processed_names:

                    time.sleep(random.uniform(0.5, 1))
                    log.debug("GETMetaData", pid=Portal.id, did=entity)
                    with Timer(key="fetchDS("+Portal.id+")") as t, Timer(key="fetchDS") as t1:
                        #fetchDataset(entity, stats, dbm, sn)
                        props={
                               'status':-1,
                               'data':None,
                               'exception':None,
                                'software':Portal.software
                               }
                        try:
                            resp, status = getPackage(apiurl=Portal.apiuri, id=entity)
                            props['status']=status
                            if resp:
                                data = resp
                                extras_to_dict(data)
                                props['data']=data
                        except Exception as e:
                            ErrorHandler.handleError(log,'c ', exception=e,pid=Portal.id, did=entity,
                               exc_info=True)
                            props['status']=getExceptionCode(e)
                            props['exception']=getExceptionString(e)

                            # if we get too much exceptions we assume this is not supported
                            not_supported_count += 1
                            if not_supported_count > NOT_SUPPORTED_PENALITY:
                                return

                        processed_names.add(entity)
                        #we always use the ckan-id for the dataset if possible
                        if props['data'] and 'id' in props['data']:
                            entity = props['data']['id']
                        d = Dataset(snapshot=sn, portalID=Portal.id, did=entity, **props)

                        p_count+=1
                        if p_count%p_steps ==0:
                            progressIndicator(p_count, tt, label=Portal.id+"_single")
                            log.info("ProgressDSFetchSingle", pid=Portal.id, processed=len(processed_ids))
                        
                        now = time.time()
                        if now-starttime>timeout:
                            timeout_counts+=1

                            if timeout_counts>TIMEOUT_PENALITY:
                                log.warning("TimeoutErrorLimitExceeded", pid=Portal.id)
                                raise TimeoutError("Timeout of "+Portal.id+" and "+str(timeout)+" seconds", timeout)
                        
                        if d.id not in processed_ids:
                            processed_ids.add(d.id)
                            yield d
                            
            progressIndicator(p_count, tt, label=Portal.id+"_single",elapsed=time.time()-tstart)
        except Exception as e:
            if len(processed_ids)==0 or isinstance(e,TimeoutError):
                raise e


class Socrata(PortalProcessor):
    def generateFetchDatasetIter(self, Portal, sn, dcat=True):
        api = urlparse.urljoin(Portal.uri, '/api/')
        page = 1
        processed=set([])

        while True:
            resp = requests.get(urlparse.urljoin(api, '/views?page=' + str(page)), verify=False)
            if resp.status_code != requests.codes.ok:
                # TODO wait? appropriate message
                pass

            res = resp.json()
            # returns a list of datasets
            if not res:
                break
            for datasetJSON in res:
                if 'id' not in datasetJSON:
                    continue

                # fetch only tabular views
                if datasetJSON.get('viewType', 'tabular') != 'tabular':
                    continue

                datasetID = datasetJSON['id']
                if datasetID not in processed:
                    processed.add(datasetID)
                    d = Dataset(snapshot=sn, portalID=Portal.id, did=datasetID, data={'view': datasetJSON}, status=200, software=Portal.software)
                    
                    if dcat:
                        try:
                            dcat_data = self._dcat(datasetID, api)
                        except Exception as e:
                            dcat_data = None
                        d.data['dcat'] = dcat_data

                    if len(processed) % 1000 == 0:
                        log.info("ProgressDSFetch", pid=Portal.id, processed=len(processed))
                    yield d
            page += 1

    def _dcat(self, id, api):
        url = urlparse.urljoin(api, 'dcat.rdf/' + id)
        resp = requests.get(url, verify=False)
        if resp.status_code == 200:
            # returns a string containing the rdf data
            return resp.text
        return None



class OpenDataSoft(PortalProcessor):
    def generateFetchDatasetIter(self, Portal, sn):
        start=0
        rows=1000000
        processed=set([])

        while True:
            query = '/api/datasets/1.0/search?rows=' + str(rows) + '&start=' + str(start)
            resp = requests.get(urlparse.urljoin(Portal.uri, query), verify=False)
            res = resp.json()
            datasets = res['datasets']
            if datasets:
                rows = len(datasets) if start==0 else rows
                start+=rows
                for datasetJSON in datasets:
                    if 'datasetid' not in datasetJSON:
                        continue
                    datasetID = datasetJSON['datasetid']

                    if datasetID not in processed:
                        data = datasetJSON
                        d = Dataset(snapshot=sn,portalID=Portal.id, did=datasetID, data=data,status=200)
                        
                        processed.add(datasetID)

                        if len(processed) % 1000 == 0:
                            log.info("ProgressDSFetch", pid=Portal.id, processed=len(processed))
                        yield d
            else:
                break


def getPackageList(apiurl):
    """ Try api 3 and api 2 to get the full package list"""
    ex =None

    status=200
    package_list=set([])
    try:
        api = ckanapi.RemoteCKAN(apiurl, get_only=True)

        start=0
        steps=1000
        while True:
            p_l = api.action.package_list(limit=steps, offset=start)
            if p_l:
                c=len(package_list)
                steps= c if start==0 else steps
                package_list.update(p_l)
                if c == len(package_list):
                    #no new packages
                    break
                start+=steps
            else:
                break
    except Exception as e:
        ErrorHandler.handleError(log, "getPackageListRemoteCKAN", exception=e, exc_info=True, apiurl=apiurl)
        ex = e

    ex1=None
    try:
        url = urlparse.urljoin(apiurl, "api/2/rest/dataset")
        resp = requests.get(url, verify=False)
        if resp.status_code == requests.codes.ok:
            p_l = resp.json()
            package_list.update(p_l)
        else:
            status = resp.status_code
    except Exception as e:
        ErrorHandler.handleError(log, "getPackageListHTTPGet", exception=e, exc_info=True,apiurl=apiurl)
        ex1=e

    if len(package_list) == 0:
        if ex1:
            raise ex1
        if ex:
            raise ex
    return package_list, status


def getPackage(apiurl, id):
    #ex =None
    #package = None
    #try:
    #    package = api.action.package_show(id=id)
    #    return package, 200
    #except Exception as e:
    #    ex = e

    #ex1=None
    try:
        url = urlparse.urljoin(apiurl, "api/2/rest/dataset/" + id)
        resp = requests.get(url, verify=False)
        if resp.status_code == requests.codes.ok:
            package = resp.json()
            return package,resp.status_code
        else:
            return None, resp.status_code
    except Exception as ex:
        ErrorHandler.handleError(log, "getPackageList", exception=ex, exc_info=True, id=id, apiurl=apiurl, excShowtype=type(ex), excShowmsg=ex.message)
        raise ex

    #we have no package
    #if ex and ex1:
    #    ErrorHandler.handleError(log, "getPackageList", exception=ex1, exc_info=True, api=api, id=id, apiurl=apiurl, excShowtype=type(ex), excShowmsg=ex.message)
    #    raise ex1
    #else:
    #    return package


class Model(object):

    def __init__(self, **kwargs):
        self.__hash= hash(pformat(kwargs))

    def __eq__(self, other):
        if isinstance(other, Model):
            return (self.__hash == other.__hash )
        else:
            return False

    def __ne__(self, other):
        return (not self.__eq__(other))

    def __hash__(self, *args, **kwargs):
        return self.__hash

class Dataset(Model):


    def __init__(self, snapshot, portalID, did, data=None, **kwargs):
        super(Dataset,self).__init__(**{'snapshot':snapshot,'portal_id':portalID,'id':did})

        self.snapshot = snapshot
        self.portal_id = portalID
        self.id = did

        self.data = data
        self.status = None
        self.exception = None
        self.md5 = None
        self.change = None
        self.qa_stats = None
        self.software= None

        for key, value in kwargs.items():
            setattr(self, key, value)