'''
Created on Aug 14, 2015

@author: jumbrich
'''
from odpw.analysers import Analyser
from test.test_zlib import ChecksumTestCase



class FetchSanity(Analyser):
    
    def __init__(self, dbm):
        self.dbm=dbm
        self.rows=[]
    
    def analyse_PortalMetaData(self, pmd):
        row = {'snapshot':pmd.snapshot, 'portalID':pmd.portal_id, 'fetch_exception':None,'fetch_processed':None}
        sanity={}
        ds={ 'idx_ds':self.dbm.countDatasetsPerSnapshot(portalID=pmd.portal_id,snapshot=pmd.snapshot),
             'pmd_ds': pmd.datasets
             }
        if pmd.fetch_stats:
            #seems we have some full fetch stats
            
            if pmd.fetch_stats.get('status', -2) != 200:
                row['fetch_exception']=  pmd.datasets ==-1
                row['fetch_processed']= True
            else:    
                """ Checks
                    1) indexed =   number of ds == indexed ds
                    2) status  =   status codes are available and sum matches
                    3) end = do we have a fetch end
                    
                    4) pmd_ds = pmd_ds <= indexed
                    5) 
                """
                
                #1
                ds['ds'] = pmd.fetch_stats['datasets'] if 'datasets' in pmd.fetch_stats else -1
                sanity['fetch_indexed'] = ds['ds'] == ds['idx_ds']
                
                #2
                resp = pmd.fetch_stats.get('respCodes',None)
                sanity['fetch_status'] =  sum(resp.values()) == ds['idx_ds'] if resp else False
                
                #3
                sanity['fetch_end'] = 'fetch_end' in pmd.fetch_stats
                   
                #4
                sanity['pmd_ds'] =  ( pmd.datasets >= ds['idx_ds'] and pmd.datasets >=ds['ds'])
                
                row['fetch_processed']= all( sanity.values())
                
                row.update(sanity)
        
        self.rows.append(row)
    
    def getResult(self):
        return {'rows':self.rows}
    
    
class HeadSanity(Analyser):
    def __init__(self, dbm):
        self.dbm=dbm
        self.rows=[]
    
    def analyse_PortalMetaData(self, pmd):
        row = {'snapshot':pmd.snapshot, 'portalID':pmd.portal_id, 'head_processed_fetch':None, 'head_processed':None}
        sanity={}
        res={ 'idx_res':self.dbm.countResourcesPerSnapshot(portalID=pmd.portal_id,snapshot=pmd.snapshot),
             'pmd_res': pmd.resources
             }
        
        if pmd.res_stats:
            
            """
                fetch process report is right
                1) urls, total, distinct
                2) total >= distinct >= urls
                3) indexed =   number of res == indexed res
                
                head script
                1) status = resp codes are availabe and sum matches
                2) size = size object and all keys are available
            """

            res['total'] = pmd.res_stats['total'] if 'total' in pmd.res_stats else -1
            res['distinct'] = pmd.res_stats['distinct'] if 'distinct' in pmd.res_stats else -1
            res['status'] = bool(pmd.res_stats['status']) if 'status' in pmd.res_stats else False
            
            sanity['head_keys'] = set(['urls', 'total', 'distinct']).issubset(pmd.res_stats)
            sanity['head_card'] = sanity['head_keys'] and  pmd.res_stats['total']>=pmd.res_stats['distinct']>=pmd.res_stats['urls']
            sanity['head_indexed'] = sanity['head_keys'] and res['idx_res'] == pmd.res_stats['distinct']
                
            row['head_processed_fetch'] = all(sanity.values())
            row.update(sanity)
            
            sanity={}
            
            resp = pmd.res_stats.get('respCodes',None)
            sanity['head_status'] =  sum(resp.values()) == res['idx_res'] if resp else False
            sanity['head_size'] = 'size' in pmd.res_stats
            
            row['head_processed'] = all(sanity.values())
            row.update(sanity)
        self.rows.append(row)
        
    
    def getResult(self):
        return {'rows':self.rows}
            