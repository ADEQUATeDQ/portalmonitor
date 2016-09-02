# -*- coding: utf-8 -*-
from bokeh.charts import Bar, Histogram, Scatter
from bokeh.embed import components
from bokeh.io import output_file, show
from bokeh.models import ColumnDataSource, Select, HBox, CustomJS
from bokeh.plotting import figure
from bokeh.resources import INLINE
from bokeh.util.browser import view
from jinja2 import Template
from sqlalchemy import func, and_

from odpw.services.aggregates import aggregatePortalInfo
from odpw.utils.plots import portalsScatter, evolutionCharts, systemEvolutionPlot
from odpw.core.db import DBClient, DBManager, query_to_dict, to_dict, row2dict
from odpw.core.model import Portal, PortalSnapshot, PortalSnapshotQuality, MetaResource, DatasetData, Dataset, \
    DatasetQuality, ResourceInfo

import pandas as pd

from odpw.utils.timing import  Timer


def showPlot(p, label="bokeh"):
    script, div = components(p)
    print div

    template = Template('''<!DOCTYPE html>
    <html lang="en">
        <head>
            <meta charset="utf-8">
            <title>Bokeh Scatter Plots</title>
            {{ js_resources }}
            {{ css_resources }}
            {{ script }}
            <style>
                .embed-wrapper {
                    width: 50%;
                    height: 400px;
                    margin: auto;
                }
            </style>
        </head>
        <body>
                <div class="embed-wrapper">

                {{ div }}

                </div>

        </body>
    </html>
    ''')

    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()

    filename = label+'.html'

    html = template.render(js_resources=js_resources,
                           css_resources=css_resources,
                           script=script,
                           div=div)

    with open(filename, 'w') as f:
        f.write(html.encode('utf-8'))

    view(filename)


def portalSize(db):

    results=[row2dict(r) for r in db.Session.query(Portal, Portal.snapshot_count,Portal.first_snapshot, Portal.last_snapshot, Portal.datasetCount, Portal.resourceCount)]
    df=pd.DataFrame(results)

    p= portalsScatter(df)
    showPlot(p,'scatter')


def evolution(db):
    data={}
    for R in db.Session.query(PortalSnapshot).filter(PortalSnapshot.portalid==portalid):
        data[R.portalid+str(R.snapshot)]=row2dict(R)
    for R in db.Session.query(PortalSnapshotQuality).filter(PortalSnapshotQuality.portalid==portalid):
        data[R.portalid+str(R.snapshot)].update(row2dict(R))

    df=pd.DataFrame([v for k,v in data.items()])
    print df

    showPlot(evolutionCharts(df))


def headstats(db):
    with Timer(verbose=True):
        print str(db.Session.query(func.count(func.distinct(MetaResource.uri))).filter(MetaResource.valid==True).join(DatasetData).join(Dataset).filter(Dataset.snapshot==1630))
    with Timer(verbose=True):
        print str(db.Session.query(func.count(func.distinct(MetaResource.uri))).filter(MetaResource.valid==True).join(Dataset, Dataset.md5==MetaResource.md5).filter(Dataset.snapshot==1630))
    with Timer(verbose=True):
        print db.statusCodeDist(1630).all()


def systemEvolution(db):

    t= db.Session.query(PortalSnapshot.snapshot.label('snapshot'), Portal.software,PortalSnapshot.datasetCount,PortalSnapshot.resourceCount).join(Portal).subquery()

    data=[ row2dict(r) for r in db.Session.query(t.c.snapshot, t.c.software, func.count().label('count'),func.sum(t.c.resourceCount).label('resources'),func.sum(t.c.datasetCount).label('datasets')).group_by(t.c.snapshot,t.c.software).all()]

    df= pd.DataFrame(data)
    print df

    p=systemEvolutionPlot(df)
    showPlot(p['datasets'])


if __name__ == '__main__':

    dbm=DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    db= DBClient(dbm)

    portalid='data_gv_at'


    systemEvolution(db)




    Timer.printStats()