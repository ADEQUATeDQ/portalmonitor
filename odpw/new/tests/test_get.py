import json
import time

import rdflib

from new.core.dataset_converter import fix_socrata_graph
from new.core.db import DBClient,DBManager
from new.core.dcat_access import getTitle

from new.core.model import DatasetData
from odpw.db.dbm import PostgressDBM


if __name__ == '__main__':

    dbm=DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')

    db= DBClient(dbm)



    md5='b8a4d9e82dc4ad70abc8997a4763ee90'
    #md5='32a9cfa856e6d9abbffd9eea8081cfd8'
    from pprint import pprint
    dataset={}
    dataset['data']=db.Session.query(DatasetData).filter(DatasetData.md5==md5).first().raw

    dataset_dict=dataset['data']

    graph = rdflib.Graph()
    if 'dcat' in dataset_dict and dataset_dict['dcat']:
            graph.parse(data=dataset_dict['dcat'], format='xml')
            fix_socrata_graph(graph, dataset_dict, 'http://example.org/')
            # TODO redesign distribution, format, contact (publisher, organization)

    format='json-ld'
    dataset['dcat']= json.loads(graph.serialize(format=format))

    title=getTitle(dataset)

    print  'dcat' in dataset
    print dataset
    print getattr(dataset,'dcat',[])
    pprint(dataset['dcat'])
    print title




