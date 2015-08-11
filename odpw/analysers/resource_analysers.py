'''
Created on Aug 7, 2015

@author: jumbrich
'''
from odpw.analysers.core import ElementCountAnalyser, DistinctElementCount
from odpw.analysers import Analyser
from odpw.utils.dataset_converter import DCAT
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
        print element.res_stats
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
 