'''
Created on Aug 7, 2015

@author: jumbrich
'''
from collections import defaultdict
from odpw.analysers.core import ElementCountAnalyser
from odpw.analysers import Analyser
import odpw.utils.util as util



  
class ResourceFormat(ElementCountAnalyser):
    """
        
    """
    def analyse_Resource(self, res):
        self.add(res.mime)
    
    def update_PortalMetaData(self, pmd):
        if not pmd.res_stats:
            pmd.res_stats = {}
        pmd.res_stats['mime'] = self.getResult()
      
#Count non empty and >0 content-length header fields
#

class ResourceSize(Analyser):
    """
    Count the non empty and >0 content-length fields and return the sum plus count
    { 'content-length':X, 'count':Y}
    """
    def __init__(self):
        self.size=0
        self.elements=0
        
    def analyse_Resource(self, element):
        if element.size and element.size>0:
            self.size+= element.size
            self.elements+=1
    
    def analyse_PortalMetaData(self, element):
        if element.res_stats and 'size' in element.res_stats:
            self.size += element.res_stats['size']['size']
            self.elements+=element.res_stats['size']['count']
            #return {'content-length':self.size,'size':util.convertSize(self.size), 'count':self.elements}
            
            
    def update_PortalMetaData(self, pmd):
        if not pmd.res_stats:
            pmd.res_stats = {}
        pmd.res_stats['size'] = self.getResult()
        print pmd.res_stats['size']    
        
    def getResult(self):
        return {'contentlength':util.convertSize(self.size),'size':self.size, 'count':self.elements}


class ResourceOverlapAnalyser(Analyser):
    """
    Builds a matrix (2-dimensional dictionary) with counts of overlapping resources
    """
    def __init__(self, filter_portal=None):
        super(ResourceOverlapAnalyser, self).__init__()
        self.filter_portal =filter_portal
        self.overlap = defaultdict(lambda: defaultdict(int))

    def analyse_Resource(self, element):
        if element.origin:
            if not self.filter_portal:
                # collect all overlap information
                for source_id in element.origin:
                    for dest_id in element.origin:
                        self.overlap[source_id][dest_id] += 1

            if self.filter_portal and self.filter_portal in element.origin:
                for pid in (id for id in element.origin if id != self.filter_portal):
                    self.overlap[self.filter_portal][pid] += 1

    def getResult(self):
        return self.overlap

    def name(self):
        name = super(ResourceOverlapAnalyser, self).name()
        if self.filter_portal:
            name += '_' + self.filter_portal
        return name


class ResourceOccurrenceCountAnalyser(ElementCountAnalyser):
    """
    Count Analyser, counting the number of occurrences for resources.
    E.g., 100 resources occur in 1500 datasets, ...
    Single Portal occurrences: number of resources occurring within a single portal (no overlap)
    """
    def __init__(self, portal_id=None):
        super(ResourceOccurrenceCountAnalyser, self).__init__()
        self.portal_id = portal_id
        self.single_portal = defaultdict(int)

    def analyse_Resource(self, element):
        if element.origin:
            if not self.portal_id:
                # count the total occurrences of a URL over all portals
                occurences = 0
                if len(element.origin) == 1:
                    # occurs in single portal
                    for pid in element.origin:
                            self.single_portal[len(element.origin[pid])] += 1
                for pid in element.origin:
                    occurences += len(element.origin[pid])
                self.add(occurences)

            if self.portal_id in element.origin:
                self.add(len(element.origin[self.portal_id]))

    def getSinglePortalOccurences(self):
        return self.single_portal

    def name(self):
        name = super(ResourceOccurrenceCountAnalyser, self).name()
        if self.portal_id:
            name += '_' + self.portal_id
        return name

class ResourceHeaderETagCountAnalyser(ElementCountAnalyser):
    def __init__(self):
        super(ResourceHeaderETagCountAnalyser, self).__init__()
        self.seen=set([])
    def analyse_Resource(self, element):
        if element.url not in self.seen:
            if element.header:
                if 'ETag' in element.header:
                    self.add(1)
                elif 'etag' in element.header:
                    self.add(1)
                else:
                    self.add(0)
            self.seen.add(element.url)
            
class ResourceHeaderLastModifiedCountAnalyser(ElementCountAnalyser):
    def __init__(self):
        super(ResourceHeaderLastModifiedCountAnalyser, self).__init__()
        self.seen=set([])
    def analyse_Resource(self, element):
        if element.url not in self.seen:
            if element.header:
                if 'Last-Modified' in element.header:
                    self.add(1)
                elif 'last-modified' in element.header:
                    self.add(1)
                else:
                    self.add(0)
            self.seen.add(element.url)

class ResourceHeaderExpiresCountAnalyser(ElementCountAnalyser):
    def __init__(self):
        super(ResourceHeaderExpiresCountAnalyser, self).__init__()
        self.seen=set([])
    def analyse_Resource(self, element):
        if element.url not in self.seen:
            if element.header:
                if 'Expires' in element.header:
                    self.add(1)
                elif 'expires' in element.header:
                    self.add(1)
                else:
                    self.add(0)
            self.seen.add(element.url)
            
class ResourceHeaderFieldsCountAnalyser(ElementCountAnalyser):
    def __init__(self):
        super(ResourceHeaderFieldsCountAnalyser, self).__init__()
        self.seen=set([])
    def analyse_Resource(self, element):
        if element.url not in self.seen:
            if element.header:
                for k in element.header:
                    self.add(k.lower().strip())
            self.seen.add(element.url)
