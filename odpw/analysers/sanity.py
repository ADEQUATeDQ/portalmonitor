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
        row = {'snapshot':pmd.snapshot, 'portalID':pmd.portal_id, 'exception':None,'processed':None}
        sanity={}
        ds={ 'idx_ds':self.dbm.countDatasetsPerSnapshot(portalID=pmd.portal_id,snapshot=pmd.snapshot),
             
             'pmd_ds': pmd.datasets
             }
        if pmd.fetch_stats:
            #seems we have some full fetch stats
            
            if pmd.fetch_stats.get('status', -2) != 200:
                row['exception']= pmd.datasets==-1
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
                sanity['indexed'] = ds['ds'] == ds['idx_ds']
                
                #2
                resp = pmd.fetch_stats.get('respCodes',None)
                sanity['status'] =  sum(resp.values()) == ds['idx_ds'] if resp else False
                
                #3
                sanity['end'] = 'fetch_end' in pmd.fetch_stats
                   
                #4
                sanity['pmd_ds'] =  ( pmd.datasets >= ds['idx_ds'] and pmd.datasets >=ds['ds'])
                
                row['processed']= all( sanity.values())
                
                row.update(sanity)
        self.rows.append(row)
        
    
    def getResult(self):
        return self.rows
    
    
class HeadSanity(Analyser):
    def __init__(self, dbm):
        self.dbm=dbm
        self.rows=[]
    
    def analyse_PortalMetaData(self, pmd):
        row = {'snapshot':pmd.snapshot, 'portalID':pmd.portal_id, 'processed_fetch':None, 'processed_head':None}
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
            
            sanity['keys'] = set(['urls', 'total', 'distinct']).issubset(pmd.res_stats)
            sanity['card'] = sanity['keys'] and  pmd.res_stats['total']>=pmd.res_stats['distinct']>=pmd.res_stats['urls']
            sanity['indexed'] = sanity['keys'] and res['idx_res'] == pmd.res_stats['distinct']
                
            row['processed_fetch'] = all(sanity.values())
            row.update(sanity)
            
            sanity={}
            
            resp = pmd.res_stats.get('status',None)
            sanity['status'] =  sum(resp.values()) == res['idx_res'] if resp else False
            sanity['size'] = 'size' in pmd.res_stats
            
            row['processed_head'] = all(sanity.values())
            row.update(sanity)
        self.rows.append(row)
        
    
    def getResult(self):
        return self.rows
            