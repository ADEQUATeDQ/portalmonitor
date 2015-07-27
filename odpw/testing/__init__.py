from odpw.db.dbm import PostgressDBM
from odpw.analysers import AnalyseEngine, QualityAnalyseEngine, Analyser

from odpw.analysers.fetching import MD5DatasetAnalyser,  ResourceInDS,\
    DatasetCount, ResourceInserter, DatasetStatusCount, DatasetAge,\
    ResourceInDSAge, KeyAnalyser, FormatCount, DatasetFetchInserter,\
    DatasetFetchUpdater
import ckanapi
import odpw.util as util

from odpw.fetch import generateFetchDatasetIter
from odpw.util import getExceptionCode, getPackageList
from odpw.db.models import PortalMetaData, Dataset, Portal

from ckanapi.errors import CKANAPIError
import requests
from odpw.analysers.quality.analysers.completeness import CompletenessAnalyser
from odpw.analysers.quality.analysers.contactability import ContactabilityAnalyser
from odpw.analysers.quality.analysers.openness import OpennessAnalyser
from odpw.analysers.quality.analysers.opquast import OPQuastAnalyser



def scan(dbm, Portal, sn):
    pmd = dbm.getPortalMetaData(portalID=Portal.id, snapshot=sn)
    if not pmd:
        pmd = PortalMetaData(portal=Portal.id, snapshot=sn)
        dbm.insertPortalMetaData(pmd)
     
    pmd.fetchstart()
    dbm.updatePortalMetaData(pmd)

    import pprint
    pprint.pprint(pmd.__dict__)
    
    ae = AnalyseEngine()
    ae.add(MD5DatasetAnalyser())
    ae.add(DatasetCount())
    ae.add(ResourceInDS(withDistinct=True))
    ae.add(ResourceInserter(dbm))
    ae.add(DatasetStatusCount())
    ae.add(ResourceInDSAge())
    ae.add(DatasetAge())
    ae.add(KeyAnalyser())
    ae.add(FormatCount())
    
    qae = QualityAnalyseEngine()
    qae.add(CompletenessAnalyser())
    qae.add(ContactabilityAnalyser())
    qae.add(OpennessAnalyser())
    qae.add(OPQuastAnalyser())
    
    
    
    
    main = AnalyseEngine()
    main.add(ae)
    main.add(qae)
    main.add(DatasetFetchUpdater(dbm))
    
    iter = Dataset.iter(dbm.getDatasets(portalID=Portal.id, snapshot=sn))
    main.process_all(iter)
    
    for ae in main.getAnalysers():
        ae.update(pmd)
        ae.update(Portal)
       
    
    #pmd.update(ae)
    pprint.pprint(pmd.__dict__)
    pprint.pprint(Portal.__dict__)
    dbm.updatePortalMetaData(pmd)
    
    

def fetching(dbm, Portal , sn):
    pmd = dbm.getPortalMetaData(portalID=Portal.id, snapshot=sn)
    if not pmd:
        pmd = PortalMetaData(portal=Portal.id, snapshot=sn)
        dbm.insertPortalMetaData(pmd)
     
    pmd.fetchstart()
    dbm.updatePortalMetaData(pmd)

    
    ae = AnalyseEngine()
    
    ae.add(MD5DatasetAnalyser())
    ae.add(DatasetCount())
    ae.add(ResourceInDS(withDistinct=True))
    ae.add(ResourceInserter(dbm))
    ae.add(DatasetStatusCount())
    ae.add(ResourceInDSAge())
    ae.add(DatasetAge())
    ae.add(KeyAnalyser())
    ae.add(FormatCount())
    ae.add(DatasetFetchInserter(dbm))

    try:
        ae.process_all(generateFetchDatasetIter(Portal, sn))
    except CKANAPIError as exc:
        Portal.status=getExceptionCode(exc)
        Portal.exception=str(type(exc))+":"+str(exc.message)
        
        print dir(exc)
        print type(exc), exc.message
    except requests.exceptions.ConnectionError as exc:
        Portal.status=getExceptionCode(exc)
        Portal.exception=str(type(exc))+":"+str(exc.message)
        print type(exc), exc.message
        print exc.response.stat_code
        
    Portal.datasets= ae.getAnalyser(DatasetCount).getResult()['count']
    Portal.resources= ae.getAnalyser(ResourceInDS).getResult()['count']
        
    pmd.fetchend()
    pmd.update(ae)


def analyseAPI(url):
    #if url == "http://catalog.data.gov/":
    #    return
    print url
    try:
        
        
        
        
        package_list, status = util.getPackageList(url)
        print "\t",len(package_list),"getPackageList", status 
    
        ps_name=[]
        ps_id=[]
        api = ckanapi.RemoteCKAN(url, get_only=True)
        response = api.action.package_search(rows=100000000)
        if response:
            datasets= response["results"]
            for ds in datasets:
                ps_name.append(ds['name'])
                ps_id.append(ds['id'])
        ps_name=set(ps_name)       
        ps_id=set(ps_id) 
        print "\t",len(ps_name),len(ps_id),"package_search"
        
        start=0
        steps=len(ps_name)
        pss_name=[]
        pss_id=[]

        while True:
            print "\t\t", steps, start
            response = api.action.package_search(rows=steps, start=start)
            if response:
                datasets= response["results"]
                if datasets:
                    for ds in datasets:
                        pss_name.append(ds['name'])
                        pss_id.append(ds['id'])
                    start+=steps
                else:
                    break
        
        pss_name=set(pss_name)       
        pss_id=set(pss_id) 
        print "\t",len(pss_name),len(pss_id),"package_search_steps"
        cpss_name=0
        cpss_id=0
        cps_name=0
        cps_id=0
        for p in package_list:
            if p in pss_name:
                cpss_name+=1
            if p in pss_id:
                cpss_id+=1
            if p in ps_name:
                cps_name+=1
            if p in ps_id:
                cps_id+=1
                
        print "\t",len(package_list)-cps_name,len(package_list)-cps_id,"ps_missing"
        print "\t",len(package_list)-cpss_name, len(package_list)-cpss_id,"pss_missing"
    except Exception as e:
        print "\t",e, e.message
def packageSearch(url):
    api = ckanapi.RemoteCKAN(url, get_only=True)
    package_list, status = util.getPackageList(url)
    print status
    start=0
    steps=1000
    p=[]
    while True:
        
        response = api.action.package_search(rows=steps, start=start)
        print start
        if response:
            start+=steps
            datasets= response["results"]
            if datasets:
                for datasetJSON in datasets:
                    datasetID = datasetJSON['name']
                
                    if datasetID in p:
                        print "seens"
                    else:
                        p.append(datasetID)
                    data = datasetJSON
                    if datasetID in package_list:
                        package_list.remove(datasetID)
                    if data['id'] in package_list:
                        package_list.remove(data['id'])
            else:
                break
        else:
            break
    
    print package_list
    print len(package_list), len(p)
if __name__ == '__main__':
    dbm= PostgressDBM(host="bandersnatch.ai.wu.ac.at", port=5433)
    
    Portal = dbm.getPortal(apiurl="http://data.wu.ac.at/")
    scan(dbm, Portal, '2015-30')
    
    
    #url="http://catalog.data.gov/"
    #analyseAPI(url)
    #for p in dbm.getPortals(maxDS=100):
        #Portal = Portal.fromResult(dict(p))
        #print Portal.id
        
        
        
        
       
        
    #packageSearch("http://data.nsw.gov.au/data/")
    #scan(dbm)