from collections import defaultdict
import csv
from odpw.analysers import Analyser, AnalyserSet, process_all
from odpw.analysers.quality.new import open_dcat_format
from odpw.db.dbm import PostgressDBM
from odpw.db.models import Dataset
from odpw.utils.licenses_mapping import LicensesOpennessMapping

__author__ = 'sebastian'


def license_report(triples):
    lm = LicensesOpennessMapping()
    with open('license_report.csv', 'w') as f:
            csvw = csv.writer(f)
            csvw.writerow(['id', 'title', 'url', 'count', 'mapping', 'conformance'])
            for id, title, url in triples.keys():
                try:
                    mapping, conformance = lm.map_license(title, id, url)
                    csvw.writerow([unicode(id).encode("utf-8"), unicode(title).encode("utf-8"), unicode(url).encode("utf-8"),
                                   triples[(id, title, url)], mapping, conformance])
                except Exception as e:
                    print e

def format_report(format_counts):
    with open('format_report.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerow(['total', 'found', 'lower', 'leading_point'])
        csvw.writerow([format_counts['total'], format_counts['found'], format_counts['lower'], format_counts['leading_point']])


class FormatOpennessMapping(Analyser):
    def __init__(self):
        self.counts = defaultdict(int)

    def getResult(self):
        return self.counts

    def analyse_Dataset(self, dataset):
        data = dataset.data
        if data:
            for resource in data.get('resources', []):
                f = resource.get('format', None)
                self.counts['total'] += 1
                if isinstance(f, basestring):
                    try:
                        f = f.strip()
                        self.counts['found'] += 1 if f in open_dcat_format.OPEN_FORMATS else 0
                        f = f.lower()
                        self.counts['lower'] += 1 if f in open_dcat_format.OPEN_FORMATS else 0
                        if f.startswith('.'):
                            f = f[1:]
                        self.counts['leading_point'] += 1 if f in open_dcat_format.OPEN_FORMATS else 0
                    except Exception as e:
                        print e


class LicenseTripleExtractor(Analyser):
    def __init__(self):
        self.triples = defaultdict(int)

    def getResult(self):
        return self.triples

    def analyse_Dataset(self, dataset):
        data = dataset.data
        if data:
            id = data.get('license_id', None)
            title = data.get('license_title', None)
            url = data.get('license_url', None)
            self.triples[(id, title, url)] += 1


if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    sn = 1537
    software = 'CKAN'

    d = dbm.getDatasetsAsStream(snapshot=sn, software=software)
    a_set = AnalyserSet()
    d1 = a_set.add(LicenseTripleExtractor())
    d2 = a_set.add(FormatOpennessMapping())

    process_all(a_set, Dataset.iter(d))

    license_report(d1.getResult())
    format_report(d2.getResult())
