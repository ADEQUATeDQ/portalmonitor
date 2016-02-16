'''
Created on Sep 15, 2015

@author: jumbrich
'''
from odpw.db.dbm import PostgressDBM
from odpw.analysers import AnalyserSet, process_all, Analyser
from odpw.analysers.core import DCATConverter
from odpw.db.models import Dataset, Portal


class MyDCATKeyAnalyser(Analyser):
    
    def analyse_Dataset(self, dataset):
        
        #get original meta data:
        metadata = dataset.data
        # NOTE; Socrata datasets have two subsets of meta data, 'dcat' and 'view'
        print metadata
        #get DCAT JSON
        dcat = dataset.dcat
        
        # do here the analysis
        

def processDatasetsBySoftware(snapshot, software):
    iter = Dataset.iter(dbm.getDatasets(snapshot=snapshot, software=software))
    
    #setup the analysers
    aset = AnalyserSet()
    #dcatconverter provides the dcat version of the metadata in jsonld
    aset.add(DCATConverter())
    dcatanalyser= aset.add(MyDCATKeyAnalyser())
    
    process_all(aset, iter)
    
    
    
def processDatasetsByPortal(snapshot, portalID):
    iter = Dataset.iter(dbm.getDatasets(snapshot=snapshot, portalID=portalID))
    
    #setup the analysers
    aset = AnalyserSet()
    #dcatconverter provides the dcat version of the metadata in jsonld
    aset.add(DCATConverter())
    dcatanalyser= aset.add(MyDCATKeyAnalyser())
    
    process_all(aset, iter)
    
    
    #processing of all datasets is done
    #maybe we want to compile and print  some stats
    #dcatanalyser.TODO 


if __name__ == '__main__':
    
    #setup DB connection and DBManager
    user="reder"
    pwd="r3d3r"
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432, password=pwd, user=user )
    
    #get iterator over datasets
    # filter by snapshot or/and software or/and portalid
    snapshot=1535 # year 2015, week 35
    snapshot=1536 # year 2015, week 36
    
    #option 1, iterate over all datasets
    software = 'CKAN' # software are one of 'CKAN', 'Socrata', 'OpenDataSoft', or None for all
    processDatasetsBySoftware(snapshot,software ) 
    
    #option 2,  iterate over all portals
    
    #get all portals, or portals by software if "software=''" is used
    portals = []
    for p in Portal.iter(dbm.getPortals()): # optional filter is software='CKAN' etc
        portals.add(p)
    #iterate over all portals
    for p in portals:
        print "analysing portal", p.id, "with software", p.software
        processDatasetsByPortal(snapshot, p.id)
    