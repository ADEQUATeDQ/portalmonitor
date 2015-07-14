'''
Created on Jul 9, 2015

@author: jumbrich
'''

from odpw.db.dbm import PostgressDBM
import pandas as pd
import vincent

from odpw.quality.analysers import AnalyseEngine, PortalSoftwareDistAnalyser,\
    PortalCountryDistAnalyser
from odpw.reports import PortalStatusReporter

if __name__ == '__main__':
    #Iterable
    
    dbm= PostgressDBM(host="localhost", port=5432)
    
    ae = AnalyseEngine()
    
    ae.add(PortalSoftwareDistAnalyser())
    ae.add(PortalStatusReporter())
    ae.add(PortalCountryDistAnalyser())
    
    ae.process_all( dbm.getPortals() )
    
    ######
    sda = ae.getAnalyser(PortalSoftwareDistAnalyser)
    
    
    #######--------------------######
    d= ae.getAnalyser(PortalCountryDistAnalyser)
    print d.getResult()
    
    geo_data = [{'name': 'countries',
             'url': "https://github.com/wrobstory/vincent_map_data/blob/master/world-countries.json",
             'feature': 'world-countries'}]

    vis = vincent.Map(geo_data=geo_data, scale=200)
    vis.to_json('portal_map.json',html_out=True,html_path='portal_map.html')
    
    
    #######--------------------######
    df = sda.getDataFrame()
    dfa=df.set_index("software")
    
    pie = vincent.Pie(dfa, inner_radius=20)
    pie.legend('Portal software')
    
    pie.to_json('software_pie.json',html_out=True,html_path='software_pie.html')
    
    
    #######--------------------######
    psr = ae.getAnalyser(PortalStatusReporter)
    
    dfa = psr.getDataFrame().copy()
    dfa=dfa.set_index('label')
    dfa= dfa.drop(['total'])
    pie = vincent.Pie(dfa, inner_radius=30, columns=['count'])
    pie.legend('Status')
    
    pie.to_json('portal_status.json',html_out=True,html_path='portal_status.html')