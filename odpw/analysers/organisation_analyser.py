from odpw.analysers.count_analysers import DatasetCount
from odpw.analysers.dataset_analysers import DatasetChangeCountAnalyser
from odpw.analysers import Analyser
from odpw.utils.dataset_converter import DCAT, FOAF, VCARD, DCT


class ISOAggregator(Analyser):
    def __init__(self, analysers):
        super(ISOAggregator, self).__init__()
        self.a=analysers
        self.per_iso={}
        self.counts={}

    def analyse_OrganisationAggregator(self, analyser):

        soft = analyser.Portal.software
        iso=analyser.Portal.iso3

        isomap= self.counts.setdefault(iso,{})
        softmap=isomap.setdefault(soft,[])
        softmap.append(analyser.Portal.id)

        #check if we have this software and iso alread, if not , create
        if iso not in self.per_iso:
            self.per_iso[iso]=[a() for a in self.a]
            self.per_iso[iso].append(DatasetChangeCountAnalyser(None))

        #iterate over the portal analysers
        for ra in analyser.a:
            for a in self.per_iso[iso]:
                a.analyse(ra)

    def done(self):
        #notify per soft dist
        for iso in self.per_iso:
            for a in self.per_iso[iso]:
                a.done()

class SoftwareAggregator(Analyser):
    def __init__(self, analysers):
        super(SoftwareAggregator, self).__init__()
        self.a=analysers
        self.per_softiso={}
        self.per_soft={}
        self.counts={}



    def analyse_OrganisationAggregator(self, analyser):

        soft = analyser.Portal.software
        iso=analyser.Portal.iso3

        softmap= self.counts.setdefault(soft,{})
        isomap=softmap.setdefault(iso,[])

        isomap.append(analyser.Portal.id)

        #check if we have this software and iso alread, if not , create
        forSoft= self.per_softiso.setdefault(soft,{})
        if iso not in self.per_softiso[soft]:
            forSoft[iso]=[a() for a in self.a]
            forSoft[iso].append(DatasetChangeCountAnalyser(None))

        #iterate over the portal analysers
        for ra in analyser.a:
            for a in self.per_softiso[soft][iso]:
                a.analyse(ra)

    def done(self):
        #notify per soft dist
        for soft in self.per_softiso:
            for iso in self.per_softiso[soft]:
                for a in self.per_softiso[soft][iso]:
                    a.done()

            #create the per software analysers
            self.per_soft[soft]=[a() for a in self.a]
            self.per_soft[soft].append(DatasetChangeCountAnalyser(None))

            for iso in self.per_softiso[soft]:
                for ra in self.per_softiso[soft][iso]:
                    for a in self.per_soft[soft]:
                        a.analyse(ra)

            for a in self.per_soft[soft]:
                a.done()


class PortalAggregator(Analyser):
    def __init__(self, analysers):
        super(PortalAggregator, self).__init__()
        self.a=[ an() for an in analysers]
        self.a.append(DatasetChangeCountAnalyser(None))


    def analyse_OrganisationAggregator(self, analyser):
        #iterate over the portal analysers
        for ra in analyser.a:
            for a in self.a:
                a.analyse(ra)
    def done(self):
        for ra in self.a:
            ra.done()

    def update(self, element):
        for ra in self.a:
            ra.update(element)

    def getResult(self):
        r={}
        for ra in self.a:
            r[ra.name()]=ra.getResult()
        return r


class OrganisationAggregator(Analyser):
    def __init__(self, Portal, snapshot,analysers):
        super(OrganisationAggregator, self).__init__()
        self.a=[ an() for an in analysers]
        self.a.append(DatasetChangeCountAnalyser(None))

        #needed for aggregation
        self.Portal= Portal
        self.snapshot= snapshot

    def addAnalyser(self,fp):
        self.a.append(fp)

    def analyse_PerOrganisationAnalyser(self, analyser):
        for org, analysers in analyser.per_orga.items():
            for a in analysers:
                for ra in self.a:
                    ra.analyse(a)
    def done(self):
        for ra in self.a:
            ra.done()

    def update(self, element):
        for ra in self.a:
            ra.update(element)

    def getResult(self):
        r={}
        for ra in self.a:
            r[ra.name()]=ra.getResult()
        return r



class PerOrganisationAnalyser(Analyser):
    def __init__(self, Portal, snapshot, analysers, datasetsfrom):
        super(PerOrganisationAnalyser, self).__init__()

        self.a=analysers
        self.Portal= Portal
        self.snapshot= snapshot
        self.per_orga={}
        self.ds=datasetsfrom

    def analyse_Dataset(self, dataset):
        org='missing'
        for dcat_el in getattr(dataset,'dcat',[]):
            #TODO there is also a FOAF.Ogranisation
            if str(FOAF.Organization) in dcat_el.get('@type',[]):
                for tag in dcat_el.get(str(FOAF.name),[]):
                    org=tag['@value']
                    break


        if org not in self.per_orga:
            self.per_orga.setdefault(org, [a() for a in self.a])
            self.per_orga[org].append(DatasetChangeCountAnalyser(self.ds))

        for a in self.per_orga[org]:
            a.analyse(dataset)

    def done(self):
        #finish per orga
        for org in self.per_orga:
            for a in self.per_orga[org]:
                a.done()





    def getResult(self):
        results={}
        for orga, analysers in self.per_orga.items():
            r=results.setdefault(orga,{})
            for a in analysers:
                r[a.name()]=a.getResult()



# class OrganisationAggregator(Analyser):
#     def __init__(self, Portal, snapshot, analysers, datasetsfrom):
#         super(OrganisationAggregator, self).__init__()
#         self.a=analysers
#         self.Portal= Portal
#         self.snapshot= snapshot
#         self.per_org={}
#         self.ds=datasetsfrom
#
#     def analyse_Dataset(self, dataset):
#         org='missing'
#         for dcat_el in getattr(dataset,'dcat',[]):
#             #TODO there is also a FOAF.Ogranisation
#             if str(FOAF.Organization) in dcat_el.get('@type',[]):
#                 for tag in dcat_el.get(str(FOAF.name),[]):
#                     org=tag['@value']
#                     break
#         if org not in self.per_org:
#             self.per_org[org]=[]
#             for a in self.a:
#                 self.per_org[org].append(a())
#             self.per_org[org].append(DatasetChangeCountAnalyser(self.ds))
#
#         for a in self.per_org[org]:
#             a.analyse(dataset)
#
#     def done(self):
#         #finish per orga
#         for org in self.per_org:
#             for a in self.per_org[org]:
#                 a.done()
#
#         #setup high level aggregators
#         self.agg_analysers=[ a() for a in self.a]
#         self.agg_analysers.append(DatasetChangeCountAnalyser(self.ds))
#         for org in self.per_org:
#             for a in self.per_org[org]:
#                 for ra in self.agg_analysers:
#                     ra.analyse(a)
#         for ra in self.agg_analysers:
#             ra.done()
#
#     def update(self, element):
#         for ra in self.agg_analysers:
#             ra.update(element)
#
#
#     def getResult(self):
#         results={'per_organisation':{}}
#         for org in self.per_org:
#             r=results['per_organisation'].setdefault(org,{})
#             for a in self.per_org[org]:
#                 r[a.name()]=a.getResult()
#
#         r=results.setdefault('total',{})
#         for ra in self.agg_analysers:
#             r[ra.name()]=ra.getResult()
#
#         return results


