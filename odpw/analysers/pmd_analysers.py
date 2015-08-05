'''
Created on Jul 30, 2015

@author: jumbrich
'''
from odpw.analysers import Analyser
from odpw.analysers.core import HistogramAnalyser



class PMDDatasetCountAnalyser(HistogramAnalyser):
    
    def analyse_PortalMetaData(self, pmd):
        ds = pmd.datasets
        if ds <=0:
            ds=0
        self.analyse_generic(ds)
    
class PMDResourceCountAnalyser(HistogramAnalyser):    
    
    def analyse_PortalMetaData(self, pmd):
        ds = pmd.resources
        if ds <=0:
            ds=0
        self.analyse_generic(ds)

class PMDActivityAnalyser(Analyser):
    
    
    times=['fetch_start', 'fetch_end']
    def __init__(self):
        self.stats=[]
        self.stats_key=['fetch_done','fetch_failed','fetch_running','fetch_missing',
                        'head_missing','head_done',
                        'quality_done']
        self.sum={}
        for k in self.stats_key:
            self.sum[k]=0
    
    def analyse_PortalMetaData(self, pmd):
        stats={ 'pid':pmd.portal_id, 'snapshot':pmd.snapshot,'fetch_error':''}
        for k in self.stats_key:
            stats[k]=None
        
        if pmd.fetch_stats:
            stats['fetch_done']=all(pmd.fetch_stats.get(k) for k in PMDActivityAnalyser.times) and pmd.fetch_stats.get('exception')==None
            stats['fetch_failed'] = all( pmd.fetch_stats.get(k) for k in ['fetch_start', 'exception'])
            for t in PMDActivityAnalyser.times:
                if t in pmd.fetch_stats:
                    stats[t]= pmd.fetch_stats[t]
            
            if stats['fetch_failed']:
                stats['fetch_error']= pmd.fetch_stats['exception'].split(":")[0].replace("<class '","").replace("'>","").replace("<type '","")
                
            stats['fetch_running'] = not stats['fetch_done'] and not stats['fetch_failed'] and  pmd.fetch_stats.get('fetch_start',None) != None
        else:
            stats['fetch_missing']=True
            
        if pmd.res_stats:
            res_total = pmd.res_stats.get('total',-1)
            res_unique = pmd.res_stats.get('unique',-1)
            
            stats['head_done']=bool(pmd.res_stats['respCodes']) if 'respCodes' in pmd.res_stats else False
            stats['head_missing']= not stats['head_done']  
        else:
            stats['head_missing']=True
            
        #if pmd.qa_stats:
            
        self.stats.append(stats)
        
        for k in self.stats_key:
            if stats[k]:
                self.sum[k] += 1    
        
    
    def done(self):
        pass
        
    def getResult(self):
        res= {'rows':self.stats, 'summary':self.sum, 'columns':self.stats_key+['pid','snapshot','fetch_error','fetch_end','fetch_start'], 'summary_columns':self.stats_key}
        print res
        return res