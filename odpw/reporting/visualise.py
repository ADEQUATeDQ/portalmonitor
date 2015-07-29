'''
Created on Jul 9, 2015

@author: jumbrich
'''

from odpw.db.dbm import PostgressDBM
import pandas as pd
import vincent



from odpw.db.models import Portal
from vincent.values import ValueRef
from numpy import shape

if __name__ == '__main__':
    #Iterable
    
    #===========================================================================
    # dbm= PostgressDBM(host="localhost", port=5432)
    # 
    # ae = AnalyseEngine()
    # 
    # ae.add(PortalSoftwareDistAnalyser())
    # 
    # ae.add(PortalCountryDistAnalyser())
    # 
    # ae.process_all( Portal.iter(dbm.getPortals()) )
    # 
    # ######
    # sda = ae.getAnalyser(PortalSoftwareDistAnalyser)
    # 
    # 
    # #######--------------------######
    # 
    # 
    # #######--------------------######
    # df = sda.getDataFrame()
    # dfa=df.set_index("software")
    # 
    # pie = vincent.Pie(dfa, inner_radius=20)
    # pie.legend('Portal software')
    # 
    # pie.to_json('software_pie.json',html_out=True,html_path='software_pie.html')
    #===========================================================================
    
    
    #######--------------------######
    
    
    #dfa = psr.getDataFrame().copy()
    #dfa=dfa.set_index('label')
    #print dfa
    
    #===========================================================================
    # dfa= dfa.drop(['total'])
    # pie = vincent.Pie(dfa, inner_radius=30, columns=['count'])
    # pie.legend('Status')
    # 
    # pie.to_json('portal_status.json',html_out=True,html_path='portal_status.html')
    #===========================================================================
    
    
    world_topo = r'world-countries.topo.json'
    geo_data = [{'name': 'countries',
             'url': world_topo,
             'feature': 'world-countries'}]

    vis = vincent.Map(geo_data=geo_data, scale=200)
    vis.to_json('portal_Mmp.json',html_out=True,html_path='portal_Mmp.html')
    
    import pandas as pd
    import vincent
    import numpy as np

    iso3 = ['USA','CHN','BRA']
    x = [50,100,150]

    data = pd.DataFrame({'iso3': iso3, 'x': x})
    
    print data
    import json
    with open('/Users/jumbrich/Dev/odpw/odpw/server/static/data/world-countries.topo.json', 'r') as f:
        get_id = json.load(f)

    new_geoms = []
    for geom in get_id['objects']['world-countries']['geometries']:
        new_geoms.append(geom['id'])

    amounts = np.zeros((shape(new_geoms)[0]))
    j=0
    for i in list(new_geoms):
        this_data = data[data.iso3==i]
        if shape(this_data)[0]>0:
            amounts[j] = np.asscalar(this_data['x'])
        else:
            amounts[j] = 0
        j=j+1

    map_data = pd.DataFrame({'iso3' : new_geoms, 'x': amounts})
    print map_data
    world_topo = r'static/data/world-countries.topo.json'
    geo_data = [{'name': 'countries',
             'url': world_topo,
            'feature': 'world-countries'}]

    mapx = vincent.Map(data=map_data, geo_data=geo_data, projection='mercator',  scale=150, 
              data_bind='x', data_key='iso3',
              map_key={'countries':'id'}, brew='YlGnBu')
    mapx.marks[0].properties.enter.stroke_opacity = vincent.ValueRef(value=.25)
    mapx.to_json('portal_Map1.json',html_out=True,html_path='portal_Map1.html')

    from pprint import pprint
    d={}
    with open("../resources/initial_data.json") as f:
        countries = json.load(f)
        
        for c in countries:
            data = c['fields']
            d[data['tld']] = data['iso3']
            
    pprint(d)
    
