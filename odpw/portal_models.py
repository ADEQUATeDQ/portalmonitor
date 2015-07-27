import urlparse
from odpw.analysers import AnalyseEngine
from odpw.db import models
from odpw.db.models import Dataset
import urllib2
import json

class PortalSoftware:
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


class CKAN(PortalSoftware):
    # TODO
    pass


class Socrata(PortalSoftware):
    def generateFetchDatasetIter(self, Portal, sn):
        api = urlparse.urljoin(Portal.url, '/api')

        # count of datasets on a portal:

        page = 1
        # returns a list of datasets
        count = 0
        while True:
            resp = urllib2.urlopen(urlparse.urljoin(api, '/views?page=' + str(page)))
            res = json.load(resp)
            if not res:
                break
            for datasetJSON in res:
                datasetID = datasetJSON['id']
                d = Dataset(snapshot=sn, portal=Portal.id, dataset=datasetID, data=datasetJSON)
                d.status = 200
                yield d

            page += 1


if __name__ == '__main__':
    ae = AnalyseEngine()
    # TODO all analysers

    p = Socrata(analyse_engine=ae)
    p.fetching(models.Portal('https://data.cdc.gov', 'https://data.cdc.gov/api'), '1')

    for a in ae.getAnalysers():
        #updatePMDwithAnalyserResults(pmd, ae)
        #pmd.update(ae)
        pass
