import copy

from analysers.dataset_analysers import DatasetChangeCountAnalyser
from odpw.analysers.count_analysers import DCATLicenseCount, DCATTagsCount, DCATOrganizationsCount, DCATFormatCount, \
    DCATDistributionCount, DatasetCount
from odpw.reporting.reporters import DBReporter,JSONReporter, Reporter, SnapshotReporter
from odpw.utils import  util






class OrgaReport(JSONReporter,SnapshotReporter):
    def __init__(self,  Portal,sn, orga, analyser  ):
        super(OrgaReport,self).__init__()
        self.Portal=Portal
        self.a=analyser
        self.sn=sn
        self.orga=orga
        self.results=[]

    def addResults(self,fp):
        self.results.append(fp)

    def jsonreport(self):
        result={'portalID':self.Portal.id
         ,'software':self.Portal.software
         ,'url':self.Portal.url
         ,'apiurl':self.Portal.apiurl
         ,'iso3':self.Portal.iso3
         ,'snapshot':self.sn
         ,'snapshot_date': util.tofirstdayinisoweek(self.sn).date()
         ,'organisation': self.orga.encode('utf-8')
         }
        for an in self.a:
            result[an.name()]=an.getResult()
        for an in self.results:
            result[an.name()]=an.getResult()
        return result

class SoftIsoReporter(JSONReporter, SnapshotReporter):
    def __init__(self,  software, iso, sn, analyser, pids  ):
        super(SoftIsoReporter,self).__init__()
        self.software=software
        self.a=analyser
        self.sn=sn
        self.iso=iso
        self.pids=pids


    def jsonreport(self):
        result={
         'software':self.software
         ,'iso3':self.iso
         ,'snapshot':self.sn
         ,'snapshot_date': util.tofirstdayinisoweek(self.sn).date()
         ,'portalIDs':self.pids
         }
        for an in self.a:
            if isinstance(an,DatasetChangeCountAnalyser):

                result[an.name()]=copy.deepcopy(an.getResult())
                del result[an.name()]['new']['values']
                del result[an.name()]['deleted']['values']
                del result[an.name()]['changed']['values']
            else:
                result[an.name()]=an.getResult()
        return result


class ISOReporter(JSONReporter, SnapshotReporter):
    def __init__(self,  iso,  sn, analyser, pidsByISO ):
        super(ISOReporter,self).__init__()
        self.iso=iso
        self.a=analyser
        self.sn=sn
        self.pidsByISO=pidsByISO

    def jsonreport(self):
        result={
         'iso':self.iso
         ,'snapshot':self.sn
         ,'snapshot_date': util.tofirstdayinisoweek(self.sn).date()
         ,'portalIDsByISO':self.pidsByISO
         }
        for an in self.a:
            if isinstance(an,DatasetChangeCountAnalyser):

                result[an.name()]=copy.deepcopy(an.getResult())
                del result[an.name()]['new']['values']
                del result[an.name()]['deleted']['values']
                del result[an.name()]['changed']['values']
            else:
                result[an.name()]=an.getResult()
        return result

class AllPortalReporter(JSONReporter, SnapshotReporter):
    def __init__(self,  sn, analyser ):
        super(AllPortalReporter,self).__init__()
        self.a=analyser
        self.sn=sn

    def jsonreport(self):
        result={
         'snapshot':self.sn
         ,'snapshot_date': util.tofirstdayinisoweek(self.sn).date()
         }
        print self.a
        for an in self.a.a:
            if isinstance(an,DatasetChangeCountAnalyser):

                result[an.name()]=copy.deepcopy(an.getResult())
                del result[an.name()]['new']['values']
                del result[an.name()]['deleted']['values']
                del result[an.name()]['changed']['values']
            else:
                result[an.name()]=an.getResult()
        return result

class PerISOReporter(Reporter):
    def __init__(self, sn, analyser ):
        super(PerISOReporter,self).__init__()
        self.a=analyser
        self.sn=sn

    def iso_reporters(self):
        for iso in self.a.per_iso:
            yield iso, ISOReporter( iso, self.sn, self.a.per_iso[iso], self.a.counts[iso])

class SoftwareReporter(JSONReporter, SnapshotReporter):
    def __init__(self,  software,  sn, analyser, pidsByISO ):
        super(SoftwareReporter,self).__init__()
        self.software=software
        self.a=analyser
        self.sn=sn
        self.pidsByISO=pidsByISO

    def jsonreport(self):
        result={
         'software':self.software
         ,'snapshot':self.sn
         ,'snapshot_date': util.tofirstdayinisoweek(self.sn).date()
         ,'portalIDsByISO':self.pidsByISO
         }
        for an in self.a:
            if isinstance(an,DatasetChangeCountAnalyser):

                result[an.name()]=copy.deepcopy(an.getResult())
                del result[an.name()]['new']['values']
                del result[an.name()]['deleted']['values']
                del result[an.name()]['changed']['values']
            else:
                result[an.name()]=an.getResult()
        return result


class PerSoftIsoReporter(Reporter):
    def __init__(self, sn, analyser ):
        super(PerSoftIsoReporter,self).__init__()
        self.a=analyser
        self.sn=sn

    def soft_iso_reporters(self):
        for soft in self.a.per_softiso:
            for iso in self.a.per_softiso[soft]:
                yield soft, iso, SoftIsoReporter(soft, iso, self.sn, self.a.per_softiso[soft][iso], self.a.counts[soft][iso])

class PerSoftwareReporter(Reporter):
    def __init__(self, sn, analyser ):
        super(PerSoftwareReporter,self).__init__()
        self.a=analyser
        self.sn=sn

    def software_reporters(self):

        for soft in self.a.per_soft:
            yield soft, SoftwareReporter(soft,  self.sn, self.a.per_soft[soft], self.a.counts[soft])





class PerOrgaReporter(Reporter):
    def __init__(self, Portal,sn, analyser ):
        super(PerOrgaReporter,self).__init__()
        self.Portal=Portal
        self.a=analyser
        self.sn=sn

    def orga_reporters(self):
        result={}
        for orga, analysers in self.a.per_orga.items():
           result[orga]=OrgaReport(self.Portal,self.sn, orga, analysers)
        return result




class PortalReporter(JSONReporter,SnapshotReporter):
    def __init__(self, Portal,sn, analysers ):
        super(PortalReporter,self).__init__()
        self.Portal=Portal
        self.a=analysers
        self.sn=sn

    def jsonreport(self):
        result={'portalID':self.Portal.id
         ,'software':self.Portal.software
         ,'url':self.Portal.url
         ,'apiurl':self.Portal.apiurl
         ,'iso3':self.Portal.iso3
         ,'snapshot':self.sn
         ,'snapshot_date': util.tofirstdayinisoweek(self.sn).date()
         }
        for an in self.a.a:
            result[an.name()]=an.getResult()

        return result




class PortalBasicReport(DBReporter, JSONReporter):

    def __init__(self, analyser, Portal):
        super(PortalBasicReport,self).__init__(analyser)
        self.Portal= Portal

    def jsonreport(self):
        df = self.getDataFrame()
        grouped = df.groupby("portal_id")
        results={}
        for portalID, group in grouped:
            results[portalID]=group['snapshot'].tolist()

        return {'portalID':self.Portal.id
         ,'software':self.Portal.software
         ,'url':self.Portal.url
         ,'apiurl':self.Portal.apiurl
         ,'iso3':self.Portal.iso3
         ,'snapshots':
             {'first': min(results[self.Portal.id])
              ,'last': max(results[self.Portal.id])
              ,'count':len(results[self.Portal.id])
              }
         }




