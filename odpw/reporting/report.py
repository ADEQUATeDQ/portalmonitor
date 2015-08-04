'''
Created on Jul 9, 2015

@author: jumbrich
'''
from collections import defaultdict
import pandas
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.fetching import CKANTagsCount, TagsCount
from odpw.analysers.socrata_analysers import SocrataTagsCount
from odpw.db.dbm import PostgressDBM
from odpw.db.models import PortalMetaData, Dataset
from odpw.reporting.reporters import ReporterEngine, Reporter, dftopk


class TagReporter(Reporter):
    def __init__(self, analyser_set):
        self.analyser = []
        for a in analyser_set.getAnalysers():
            if isinstance(a, TagsCount):
                self.analyser.append(a)
        self.df = None

    def getDataFrame(self):
        if self.df is None:
            data = defaultdict(int)
            for a in self.analyser:
                res = a.getResult()
                for k in res:
                    data[k] += res[k]
            self.df = pandas.DataFrame(data.items(), columns=['Tag', 'Count'])
        return self.df




if __name__ == '__main__':
    
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)


    portals = dbm.getPortals(software='Socrata')
    tags = []
    for p in portals:
        a1 = AnalyserSet()
        ta = SocrataTagsCount()
        a1.add(ta)
        ds = dbm.getDatasets(portalID=p.id, snapshot=1531)
        process_all(a1, Dataset.iter(ds))
        tags.append(ta)
    portals = dbm.getPortals(software='CKAN')
    for p in portals:
        a1 = AnalyserSet()
        ta = CKANTagsCount()
        a1.add(ta)
        ds = dbm.getDatasets(portalID=p.id, snapshot=1531)
        process_all(a1, Dataset.iter(ds))
        tags.append(ta)

    # pmds = [dbm.getPortalMetaData(p.id, ) for p in portals]

    ta = TagsCount()
    #ae.add(cta)

    a2 = AnalyserSet()
    a2.add(ta)

    process_all(a2, tags)

    tags = TagReporter(a2)

    re = ReporterEngine(tags)
    tmp = dftopk(tags.getDataFrame(), 'Count', k=50)
    print tmp

