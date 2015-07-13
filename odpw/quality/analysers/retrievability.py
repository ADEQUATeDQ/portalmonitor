__author__ = 'jumbrich'

DS = 'ds'
RES = 'res'

class RetrievabilityAnalyser:

    def __init__(self, dbm, portal_id, snapshot):
          #retrieval stats
        self.fetch_stats = {
            DS: {},
            RES: {}
        }
        self.total = 0.0
        self.quality = {DS: 0, RES: 0}
        self.dbm = dbm
        self.portal_id = portal_id
        self.snapshot = snapshot


    def visit(self, dataset):
        #update the fetch statistics
        self.updateFetchStats(self.fetch_stats[DS], str(dataset['respCode']))



    def update(self, PMD):
        PMD.__dict__['fetch_stats'] = self.fetch_stats
        PMD.__dict__['quality']['Qr'] = self.quality


    def computeSummary(self):
        if self.total > 0:
            self.quality[DS] = self.fetch_stats[DS].get('200',0) / self.total
        else:
            self.quality[DS] = 0

        self.quality[RES] = self.computeResourceAccess()

    def updateFetchStats(self, stats, value):
        count = stats.get(value,0)
        stats[value] = count +1
        self.total += 1

    def computeResourceAccess(self):
        statuses = self.dbm.getResourceStatusByPortalSnapshot(self.portal_id, self.snapshot)
        count = 0.0
        ok = 0.0
        for s in statuses:
            if 'crawl_meta_data' in s and 'status' in s['crawl_meta_data']:
                status = s['crawl_meta_data']['status']
                stats_count = self.fetch_stats[RES].get(str(status), 0)
                self.fetch_stats[RES][str(status)] = stats_count + 1
                count += 1
                if status == 200:
                    ok += 1
        if count > 0:
            return ok / count
        return None
