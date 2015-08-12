from ast import literal_eval
import random
import urlparse
import ckanapi
import time
import requests

from odpw.utils import util
from odpw.db.models import Dataset
from odpw.utils.timer import Timer
from odpw.utils.util import ErrorHandler as eh, progressIndicator, TimeoutError,\
    ErrorHandler

import structlog
log = structlog.get_logger()

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
        api = ckanapi.RemoteCKAN(Portal.apiurl, get_only=True)
        start=0
        rows=1000

        p_count=0
        p_steps=1
        total=0
        processed_ids=set([])
        processed_names=set([])
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
                                util.extras_to_dicts(data)
                            
                                d = Dataset(snapshot=sn, portalID=Portal.id, did=datasetID, data=data, status=200, software=Portal.software)
                                processed_ids.add(d.id)
                                processed_names.add(datasetJSON['name'])
                                
                                p_count+=1
                                if p_count%p_steps ==0:
                                    progressIndicator(p_count, total, label=Portal.id)
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
            progressIndicator(p_count, total, label=Portal.id+"_batch")
            #if len(processed) == total:
            #    #assuming that package_search['count']
            #    return
        except TimeoutError as e:
            raise e
        except Exception as e:
            ErrorHandler.handleError(log,"CKANDSFetchBatchError", pid=Portal.id, exception=e, exc_info=True)

        try:
            package_list, status = util.getPackageList(Portal.apiurl)
            if total >0 and len(package_list) !=total:
                log.info("PackageList_COUNT", total=total, pid=Portal.id, pl=len(package_list))
            #len(package_list)
            tt=len(package_list)
            p_steps=tt/100
            if p_steps == 0:
                p_steps=1
            p_count=0
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
                            resp, status = util.getPackage( apiurl=Portal.apiurl, id=entity)
                            props['status']=status
                            if resp:
                                data = resp
                                util.extras_to_dict(data)
                                props['data']=data
                        except Exception as e:
                            eh.handleError(log,'FetchDataset', exception=e,pid=Portal.id, did=entity,
                               exc_info=True)
                            props['status']=util.getExceptionCode(e)
                            props['exception']=util.getExceptionString(e)

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
                            raise TimeoutError("Timeout of "+Portal.id+" and "+str(timeout)+" seconds", timeout)
                        
                        if d.id not in processed_ids:
                            processed_ids.add(d.id)
                            yield d
                            
            progressIndicator(p_count, tt, label=Portal.id+"_single")
        except Exception as e:
            if len(processed_ids)==0:
                raise e


class Socrata(PortalProcessor):
    def generateFetchDatasetIter(self, Portal, sn, dcat=True):
        api = urlparse.urljoin(Portal.url, '/api/')
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
            resp = requests.get(urlparse.urljoin(Portal.url, query), verify=False)
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

