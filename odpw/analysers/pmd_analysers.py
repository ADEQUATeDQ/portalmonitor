'''
Created on Jul 30, 2015

@author: jumbrich
'''
from collections import defaultdict
import numpy as np
from odpw.analysers import Analyser
from odpw.analysers.core import HistogramAnalyser
from odpw.analysers.fetching import UsageAnalyser
from odpw.analysers.quality.analysers.completeness import CompletenessAnalyser
from odpw.analysers.quality.analysers.contactability import ContactabilityAnalyser
from odpw.analysers.quality.analysers.openness import OpennessAnalyser


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
                        'head_missing','head_done','head_running'
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

class MultiHistogramAnalyser(Analyser):
    def __init__(self, **nphistparams):
        # should be key value pairs of description + list
        self.data = defaultdict(list)
        self.nphistparams=nphistparams

    def getResult(self):
        result = {}
        for d in self.data:
            hist, bin_edges = np.histogram(np.array(self.data[d]), **self.nphistparams)
            result[d] = {'hist': hist, 'bin_edges': bin_edges}
        return result

class CompletenessHistogram(MultiHistogramAnalyser):
    def analyse_PortalMetaData(self, pmd):
        if pmd.qa_stats and CompletenessAnalyser.id in pmd.qa_stats:
            quality = pmd.qa_stats[CompletenessAnalyser.id]
            for group in quality:
                self.data[group].append(quality[group])

    def analyse_Dataset(self, dataset):
        if dataset.qa_stats and CompletenessAnalyser.id in dataset.qa_stats:
            quality = dataset.qa_stats[CompletenessAnalyser.id]
            for group in quality:
                self.data[group].append(quality[group])

class ContactabilityHistogram(MultiHistogramAnalyser):
    def analyse_PortalMetaData(self, pmd):
        if pmd.qa_stats and ContactabilityAnalyser.id in pmd.qa_stats:
            contact = pmd.qa_stats[ContactabilityAnalyser.id]
            for root in contact:
                for child in contact[root]:
                    self.data[root + '_' + child].append(contact[root][child])

    def analyse_Dataset(self, dataset):
        if dataset.qa_stats and ContactabilityAnalyser.id in dataset.qa_stats:
            contact = dataset.qa_stats[ContactabilityAnalyser.id]
            for root in contact:
                for child in contact[root]:
                    self.data[root + '_' + child].append(contact[root][child])


class OpennessHistogram(MultiHistogramAnalyser):
    def analyse_PortalMetaData(self, pmd):
        if pmd.qa_stats and OpennessAnalyser.id in pmd.qa_stats:
            quality = pmd.qa_stats[OpennessAnalyser.id]
            for group in quality:
                self.data[group].append(quality[group])

    def analyse_Dataset(self, dataset):
        if dataset.qa_stats and OpennessAnalyser.id in dataset.qa_stats:
            quality = dataset.qa_stats[OpennessAnalyser.id]
            for group in quality:
                self.data[group].append(quality[group])


class UsageHistogram(MultiHistogramAnalyser):
    def analyse_PortalMetaData(self, pmd):
        if pmd.qa_stats and UsageAnalyser.id in pmd.qa_stats:
            quality = pmd.qa_stats[UsageAnalyser.id]
            for group in quality:
                self.data[group].append(quality[group])

    def analyse_Dataset(self, dataset):
        if dataset.qa_stats and UsageAnalyser.id in dataset.qa_stats:
            quality = dataset.qa_stats[UsageAnalyser.id]
            for group in quality:
                self.data[group].append(quality[group])


class AccuracyHistogram(MultiHistogramAnalyser):
    def __init__(self, **nphistparams):
        super(AccuracyHistogram, self).__init__(**nphistparams)
        self.counts = defaultdict(list)

    def getCounts(self):
        return self.counts

    def analyse_dict(self, element):
        for p in element:
            quality = element[p]
            form_cont_count = quality['format']['content']['count']
            form_cont = quality['format']['content']['score']
            form_header_count = quality['format']['header']['count']
            form_header = quality['format']['header']['score']

            if form_cont and form_header:
                cont_total = form_cont * form_cont_count
                head_total = form_header * form_header_count

                self.data['qaf'].append((cont_total + head_total)/(form_cont_count + form_header_count))
                self.counts['qaf'].append(max(form_cont_count, form_header_count))

            elif form_cont:
                self.data['qaf'].append(form_cont)
                self.counts['qaf'].append(form_cont_count)
            elif form_header:
                self.data['qaf'].append(form_header)
                self.counts['qaf'].append(form_header_count)

            if quality['mime_type']['header']['score']:
                self.data['qam'].append(quality['mime_type']['header']['score'])
                self.counts['qam'].append(quality['mime_type']['header']['count'])

            if quality['size']['header']['score']:
                self.data['qas'].append(quality['size']['header']['score'])
                self.counts['qas'].append(quality['size']['header']['count'])

