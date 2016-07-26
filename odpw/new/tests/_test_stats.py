# -*- coding: utf-8 -*-
from bokeh.charts import Bar, Histogram, Scatter
from bokeh.embed import components
from bokeh.io import output_file, show
from bokeh.models import ColumnDataSource, Select, HBox, CustomJS
from bokeh.plotting import figure
from bokeh.resources import INLINE
from bokeh.util.browser import view
from jinja2 import Template

from odpw.new.services.aggregates import aggregatePortalInfo
from odpw.new.utils.plots import portalsScatter, evolutionCharts
from odpw.new.core.db import DBClient, DBManager, query_to_dict, to_dict, row2dict
from odpw.new.core.model import Portal, PortalSnapshot, PortalSnapshotQuality

import pandas as pd

from odpw.new.utils.timing import  Timer


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




if __name__ == '__main__':

    dbm=DBManager(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    db= DBClient(dbm)

    portalid='data_gv_at'

    #portalSize(db)

    snapshot=1628
    import pprint
    #pprint.pprint(aggregatePortalInfo(db,portalid,snapshot))

    data={}
    for R in db.Session.query(PortalSnapshot).filter(PortalSnapshot.portalid==portalid):
        data[R.portalid+str(R.snapshot)]=row2dict(R)
    for R in db.Session.query(PortalSnapshotQuality).filter(PortalSnapshotQuality.portalid==portalid):
        data[R.portalid+str(R.snapshot)].update(row2dict(R))

    df=pd.DataFrame([v for k,v in data.items()])
    print df

    showPlot(evolutionCharts(df))

    Timer.printStats()