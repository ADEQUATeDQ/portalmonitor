'''
Created on Oct 14, 2015

@author: jumbrich
'''
from odpw.db.dbm import PostgressDBM
from odpw.reporting.activity_reports import fetch_process
import os
from odpw.reporting.info_reports import systeminfoall

if __name__ == '__main__':
    
    
    "Generating plots and tables for the JDIQ paper"
    
    #snapshots we consider for now'
    snapshots=[1533, 1534, 1535, 1536, 1537, 1538, 1539, 1540, 1541,1542]
    #snapshots=[1533]
    
    
    jdiq = "/Users/jumbrich/Documents/opodportal_docs/journal/jdiq"
    plot_folder=os.path.join(jdiq,"plots")
    table_folder=os.path.join(jdiq,"tables")
    
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    
    #PROCESSING
    report = fetch_process(dbm, snapshots)
    
    report.plotreport(plot_folder)
    report.textablereport(table_folder)
    
    #system start 
    
    #report = systeminfoall(dbm)
    #report.textablereport(table_folder)