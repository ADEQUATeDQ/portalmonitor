'''
Created on Jul 9, 2015

@author: jumbrich
'''

from odpw.db.dbm import PostgressDBM
import pandas as pd
import vincent

from odpw.analysers import AnalyseEngine, PortalSoftwareDistAnalyser,\
    PortalCountryDistAnalyser

from odpw.db.models import Portal

if __name__ == '__main__':
    #Iterable
    
    dbm= PostgressDBM(host="localhost", port=5432)
    
    ae = AnalyseEngine()
    
    ae.add(PortalSoftwareDistAnalyser())
    
    ae.add(PortalCountryDistAnalyser())
    
    ae.process_all( Portal.iter(dbm.getPortals()) )
    
    ######
    sda = ae.getAnalyser(PortalSoftwareDistAnalyser)
    
    
    #######--------------------######
    
    
    #######--------------------######
    df = sda.getDataFrame()
    dfa=df.set_index("software")
    
    pie = vincent.Pie(dfa, inner_radius=20)
    pie.legend('Portal software')
    
    pie.to_json('software_pie.json',html_out=True,html_path='software_pie.html')
    
    
    #######--------------------######
    
    
    #dfa = psr.getDataFrame().copy()
    #dfa=dfa.set_index('label')
    #print dfa
    
    dfa= dfa.drop(['total'])
    pie = vincent.Pie(dfa, inner_radius=30, columns=['count'])
    pie.legend('Status')
    
    pie.to_json('portal_status.json',html_out=True,html_path='portal_status.html')