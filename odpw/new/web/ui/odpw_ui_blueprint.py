import datetime
from urlparse import urlparse

import jinja2
import pandas as pd
from bokeh.embed import components
from bokeh.resources import INLINE
from flask import Blueprint, current_app, render_template

from odpw.new.services.aggregates import aggregatePortalInfo

from odpw.new.core.model import Portal, PortalSnapshotQuality, PortalSnapshot
from odpw.new.core.db import row2dict

from odpw.new.utils.utils_snapshot import getWeekString, getSnapshotfromTime, getPreviousWeek, getNextWeek
from odpw.new.utils.timing import Timer

from odpw.new.web.ui.plots import fetchProcessChart, qualityChart, qa
from odpw.new.web.cache import cache

ui = Blueprint('ui', __name__,
                    template_folder='../templates',
                    static_folder='../static',
                    )

# using the method
@jinja2.contextfilter
def get_domain(context, url):
        return "%s" % urlparse(url).netloc

ui.add_app_template_filter(get_domain)
ui.add_app_template_filter(getWeekString)


@ui.route('/', methods=['GET'])
def help():
    return render_template('index.jinja')

@ui.route('/spec', methods=['GET'])
def spec():
    return render_template('spec.json', host="localhost:5122/", basePath="api")

@ui.route('/api', methods=['GET'])
def api():
    return render_template('apiui.jinja')

@ui.route('/timer', methods=['GET'])
def timer():
    return render_template('timer.jinja', stats=Timer.getStats())


@ui.route('/portals', methods=['GET'])
def portals():
    with Timer(key="portals"):
        r=current_app.config['dbsession'].query(Portal, Portal.snapshot_count,Portal.first_snapshot, Portal.last_snapshot, Portal.datasetCount, Portal.resourceCount)
        ps=[]
        for P in r:
            #print 'P',P
            d={}
            d.update(row2dict(P[0]))
            d['snCount']=P[1]
            d['snFirst']=P[2]
            d['snLast']=P[3]
            d['datasets']=P[4]
            d['resources']=P[5]
            d['licenses']=current_app.config['dbc'].distinctLicenses(snapshot=P[3], portalid=P[0].id).count()
            print d
            ps.append(d)
        return render_template('odpw_portals.jinja', data=ps)

@ui.route('/system', methods=['GET'])
def system():
    with Timer(key="system"):
        return render_template("odpw_system_info.jinja")

@ui.route('/system/fetch', methods=['GET'])
def systemfetch():
    with Timer(key="systemfetch"):
        db=current_app.config['dbc']
        cursn=getSnapshotfromTime(datetime.datetime.now())
        p= fetchProcessChart(db,cursn)
        script, div= components(p)

        js_resources = INLINE.render_js()
        css_resources = INLINE.render_css()

        return render_template("odpw_system_fetch.jinja",
            plot_script=script,
            plot_div=div,
            js_resources=js_resources,
            css_resources=css_resources
        )


#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
### PORTAL
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
@cache.cached(timeout=300)
def getPortalInfos(db, portalid, snapshot):
    with Timer(key="getPortalInfos"):
        snapshots=[i[0] for i in db.Session.query(PortalSnapshot.snapshot).filter(PortalSnapshot.portalid==portalid).all()]

        p=getPreviousWeek(snapshot)
        p=p if p in snapshots else None
        n=getNextWeek(snapshot)
        n=n if n in snapshots else None
        data={'snapshots':{'list':snapshots,'prev':p, 'next': n}}

        return data

def getResourceInfo(db, portalid, snapshot):
    data={}

    data['valid']={}
    for valid in db.validURLDist(snapshot, portalid=portalid):
        data['valid'][valid[0]]=valid[1]
    data['status']={}
    for res in db.statusCodeDist(snapshot,portalid=portalid):
        data['status'][res[0]]=res[1]

    return {'resources':data}





@ui.route('/portal/<portalid>/<int:snapshot>', methods=['GET'])
def portal(snapshot, portalid):
    with Timer(key="portal"):
        db=current_app.config['dbc']

        data=getPortalInfos(db,portalid,snapshot)

        r=current_app.config['dbsession'].query(Portal, Portal.datasetCount, Portal.resourceCount).filter(Portal.id==portalid)
        ps=[]
        for P in r:
            data.update(row2dict(P[0]))
            data['datasets']=P[1]
            data['resources']=P[2]
        data.update(getResourceInfo(db,portalid,snapshot))
        data.update(aggregatePortalInfo(db,portalid,snapshot))


        return render_template("odpw_portal.jinja",  snapshot=snapshot, portalid=portalid,data=data)


@ui.route('/portal/<portalid>/<int:snapshot>/resources', methods=['GET'])
def portalRes(snapshot, portalid):
    db=current_app.config['dbc']

    data=getResourceInfo(db, portalid, snapshot)

    data['uris']= [row2dict(i) for i in db.getResourceInfos(snapshot,portalid=portalid) ]

    data.update(getPortalInfos(db,portalid,snapshot))
    return render_template("odpw_portal_resources.jinja", data=data, snapshot=snapshot, portalid=portalid)

@ui.route('/portal/<portalid>/<int:snapshot>/quality', methods=['GET'])
def portalQuality(snapshot, portalid):

    db=current_app.config['dbc']
    data=None
    for r in db.Session.query(PortalSnapshotQuality)\
        .filter(PortalSnapshotQuality.portalid==portalid)\
        .filter(PortalSnapshotQuality.snapshot==snapshot):
        data=row2dict(r)
        break
    d=[]

    datasets= int(data['datasets'])
    for inD in qa:
        for k , v in inD['metrics'].items():
            k=k.lower()
            value=float(data[k])
            perc=int(data[k+'N'])/(datasets*1.0) if datasets>0 else 0
            c= { 'Metric':k, 'Dimension':inD['dimension'],
                 'dim_color':inD['color'], 'value':value, 'perc':perc}
            c.update(v)
            d.append(c)
    df = pd.DataFrame(d)

    with Timer(key="dataDF", verbose=True) as t:
        p= qualityChart(df)

    script, div= components(p)

    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()

    db=current_app.config['dbc']
    data = getPortalInfos(db,portalid,snapshot)
    return render_template("odpw_portal_quality.jinja",
        plot_script=script
        ,plot_div=div
        ,js_resources=js_resources
        ,css_resources=css_resources
        ,snapshot=snapshot
        , portalid=portalid
        , data=data
    )
