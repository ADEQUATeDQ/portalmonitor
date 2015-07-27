import random
import urlparse
import ckanapi
import time
from odpw import util
from odpw.db import models
from odpw.db.models import Dataset
import urllib2
import json
from odpw.timer import Timer
from odpw.util import ErrorHandler as eh

from odpw.analysers import AnalyseEngine

import logging
logger = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()


class PortalProcessor:
    """
    abstract super class for different portal software (e.g., Socrata, CKAN)
    """

    def __init__(self, analyse_engine):
        self.analyse_engine = analyse_engine

    def generateFetchDatasetIter(self, Portal, sn):
        raise NotImplementedError("Should have implemented this")

    def fetching(self, Portal, sn):
        Portal.latest_snapshot=sn

        iter = self.generateFetchDatasetIter(Portal, sn)
        self.analyse_engine.process_all(iter)




class CKAN(PortalProcessor):
    def generateFetchDatasetIter(self, Portal, sn):
        api = ckanapi.RemoteCKAN(Portal.apiurl, get_only=True)
        start=0
        rows=1000000

        processed=set([])

        while True:
            response = api.action.package_search(rows=rows, start=start)
            #print Portal.apiurl, start, rows, len(processed)
            datasets = response["results"] if response else None
            if datasets:
                rows = len(datasets) if start==0 else rows
                start+=rows
                for datasetJSON in datasets:
                    datasetID = datasetJSON['name']

                    if datasetID not in processed:
                        data = datasetJSON

                        d = Dataset(snapshot=sn,portal=Portal.id, dataset=datasetID, data=data)
                        d.status=200

                        processed.add(datasetID)

                        if len(processed) % 1000 == 0:
                            log.info("ProgressDSFetch", pid=Portal.id, processed=len(processed))

                        yield d

            else:
                break
        try:
            package_list, status = util.getPackageList(Portal.apiurl)
            total=len(package_list)

            for entity in package_list:
                #WAIT between two consecutive GET requests
                if entity not in processed:
                    processed.add(d.dataset)

                    time.sleep(random.uniform(0.5, 1))
                    log.debug("GETMetaData", pid=Portal.id, did=entity)
                    with Timer(key="fetchDS("+Portal.id+")") as t, Timer(key="fetchDS") as t1:
                        #fetchDataset(entity, stats, dbm, sn)
                        props={
                               'status':-1,
                               'data':None,
                               'exception':None
                               }
                        try:
                            resp = api.action.package_show(id=entity)
                            data = resp
                            util.extras_to_dict(data)
                            props['data']=data
                            props['status']=200
                        except Exception as e:
                            eh.handleError(log,'FetchDataset', exception=e,pid=Portal.id, did=entity,
                               exc_info=True)
                            props['status']=util.getExceptionCode(e)
                            props['exception']=str(type(e))+":"+str(e.message)

                        d = Dataset(snapshot=sn,portal=Portal.id, dataset=entity, **props)
                        processed.add(d.dataset)

                        if len(processed) % 1000 == 0:
                            log.info("ProgressDSFetch", pid=Portal.id, processed=len(processed))

                        yield d

        except Exception as e:
            if len(processed)==0:
                raise e


class Socrata(PortalProcessor):
    def generateFetchDatasetIter(self, Portal, sn):
        api = urlparse.urljoin(Portal.url, '/api')
        page = 1
        count = 0
        while True:
            resp = urllib2.urlopen(urlparse.urljoin(api, '/views?page=' + str(page)))
            # returns a list of datasets
            res = json.load(resp)
            if not res:
                break
            for datasetJSON in res:
                datasetID = datasetJSON['id']
                d = Dataset(snapshot=sn, portal=Portal.id, dataset=datasetID, data=datasetJSON)
                d.status = 200
                count += 1
                if count % 1000 == 0:
                    log.info("ProgressDSFetch", pid=Portal.id, processed=count)
                yield d

            page += 1


class OpenDataSoft(PortalProcessor):
    def generateFetchDatasetIter(self, Portal, sn):
        start=0
        rows=1000000

        while True:
            query = '/api/datasets/1.0/search?rows=' + str(rows) + '&start=' + str(start)
            resp = urllib2.urlopen(urlparse.urljoin(Portal.url, query))
            datasets = None
            if datasets:
                pass
            else:
                break


if __name__ == '__main__':
    ae = AnalyseEngine()
    # TODO all analysers

    p = Socrata(analyse_engine=ae)
    p.fetching(models.Portal('https://data.cdc.gov', 'https://data.cdc.gov/api'), '1')

    for a in ae.getAnalysers():
        #updatePMDwithAnalyserResults(pmd, ae)
        #pmd.update(ae)
        pass
