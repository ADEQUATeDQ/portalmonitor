import ast
import datetime
import json
from urlparse import urlparse

import jinja2
import pandas as pd
from bokeh.embed import components
from bokeh.resources import INLINE
from flask import Blueprint, current_app, render_template, jsonify
from markupsafe import Markup
from sqlalchemy import func

from odpw.new.utils.plots import fetchProcessChart, qualityChart, qa, portalsScatter, evolutionCharts
from odpw.new.utils.statistics import portalSnapshotQualityDF
from odpw.new.core.db import row2dict
from odpw.new.core.model import Portal, PortalSnapshotQuality, PortalSnapshot, Dataset, DatasetData, DatasetQuality
from odpw.new.services.aggregates import aggregatePortalInfo
from odpw.new.utils.timing import Timer
from odpw.new.utils.utils_snapshot import getWeekString, getSnapshotfromTime, getPreviousWeek, getNextWeek
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
def index():
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

@ui.route('/system', methods=['GET'])
def system():
    with Timer(key="system" , verbose=True):
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

#@cache.cached(timeout=300)
def getPortalsInfo():

    with Timer(key="portals", verbose=True):
        ps=[]
        r=current_app.config['dbsession'].query(Portal, Portal.snapshot_count,Portal.first_snapshot, Portal.last_snapshot, Portal.datasetCount, Portal.resourceCount)
        print str(r)
        for P in r:
            #print 'P',P
            d={}
            d.update(row2dict(P[0]))
            d['snCount']=P[1]
            d['snFirst']=P[2]
            d['snLast']=P[3]
            d['datasets']=P[4]
            d['resources']=P[5]
            #with Timer(key="portalsLicenses"):
            #   d['licenses']=current_app.config['dbc'].distinctLicenses(snapshot=P[3], portalid=P[0].id).count()
            #print d
            ps.append(d)
    return ps

@ui.route('/portalslist', methods=['GET'])
#@cache.cached(timeout=300)
def portalslist():
    ps=getPortalsInfo()
    return render_template('odpw_portals.jinja', data=ps)

@ui.route('/portalstable', methods=['GET'])
#@cache.cached(timeout=300)
def portalstable():
    ps=getPortalsInfo()
    return render_template('odpw_portals_table.jinja', data=ps)


@ui.route('/portals/portalsquality', methods=['GET'])
#@cache.cached(timeout=300)
def portalsquality():
    with Timer(key="portalsquality", verbose=True):
        db=current_app.config['dbc']

        snapshot=getPreviousWeek(getSnapshotfromTime(datetime.datetime.now()))
        results=[row2dict(r) for r in db.Session.query(Portal, Portal.datasetCount, Portal.resourceCount).join(PortalSnapshotQuality).filter(PortalSnapshotQuality.snapshot==1628).add_entity(PortalSnapshotQuality)]

        keys=[ i.lower() for q in qa for i in q['metrics'] ]
        df=pd.DataFrame(results)
        for c in keys:
            df[c]=df[c].convert_objects(convert_numeric=True)

        dfiso= df.groupby(['iso'])
        dfiso=dfiso.agg('mean')\
             .join(pd.DataFrame(dfiso.size(),columns=['count']))
        resultsIso= dfiso.reset_index().to_dict(orient='records')

        dfsoft= df.groupby(['software'])
        dfsoft=dfsoft.agg('mean')\
             .join(pd.DataFrame(dfsoft.size(),columns=['count']))
        resultSoft= dfsoft.reset_index().to_dict(orient='records')

        return render_template('odpw_portals_quality.jinja', data={'portals':results,'iso':resultsIso,'soft':resultSoft}, keys=keys, snapshot=snapshot)

@ui.route('/portals/portalsstats', methods=['GET'])
#@cache.cached(timeout=300)
def portalssize():
    with Timer(key="portalsstats", verbose=True):
        db=current_app.config['dbc']


        results=[row2dict(r) for r in db.Session.query(Portal, Portal.snapshot_count,Portal.first_snapshot, Portal.last_snapshot, Portal.datasetCount, Portal.resourceCount)]
        df=pd.DataFrame(results)

        p= portalsScatter(df)
        script, div= components(p)

        js_resources = INLINE.render_js()
        css_resources = INLINE.render_css()

        return render_template("odpw_portals_stats.jinja",
            plot_script=script,
            plot_div=div,
            js_resources=js_resources,
            css_resources=css_resources
        )


#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
### PORTAL
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
@cache.cached(timeout=300)
def getPortalInfos(Session, portalid, snapshot):
    with Timer(key="getPortalInfos", verbose=True):
        snapshots=[i[0] for i in Session.query(PortalSnapshot.snapshot).filter(PortalSnapshot.portalid==portalid).all()]

        p=getPreviousWeek(snapshot)
        p=p if p in snapshots else None
        n=getNextWeek(snapshot)
        n=n if n in snapshots else None
        data={'snapshots':{'list':snapshots,'prev':p, 'next': n}}
        data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
        return data

def getResourceInfo(db, portalid, snapshot):

    with Timer(key="getResourceInfo",verbose=True):
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
    with Timer(key="portal",verbose=True):
        Session=current_app.config['dbsession']
        db=current_app.config['dbc']
        data=getPortalInfos(Session,portalid,snapshot)

        with Timer(key="portalQuery",verbose=True):
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
    with Timer(key="portalRes",verbose=True):
        db=current_app.config['dbc']

        data=getResourceInfo(db, portalid, snapshot)

        data['uris']= [row2dict(i) for i in db.getResourceInfos(snapshot,portalid=portalid) ]

        Session=current_app.config['dbsession']
        data.update(getPortalInfos(Session,portalid,snapshot))

        return render_template("odpw_portal_resources.jinja", data=data, snapshot=snapshot, portalid=portalid)



@ui.route('/portal/<portalid>/<int:snapshot>/evolution', methods=['GET'])
def portalEvolution(snapshot, portalid):
    with Timer(key="portalEvolution",verbose=True):
        db=current_app.config['dbc']
        data={}
        for R in db.Session.query(PortalSnapshot).filter(PortalSnapshot.portalid==portalid):
            data[R.portalid+str(R.snapshot)]=row2dict(R)
        for R in db.Session.query(PortalSnapshotQuality).filter(PortalSnapshotQuality.portalid==portalid):
            data[R.portalid+str(R.snapshot)].update(row2dict(R))

        df=pd.DataFrame([v for k,v in data.items()])
        p=evolutionCharts(df)
        script, div= components(p)

        js_resources = INLINE.render_js()
        css_resources = INLINE.render_css()

        db=current_app.config['dbc']
        Session=current_app.config['dbsession']
        data = getPortalInfos(Session,portalid,snapshot)

        return render_template("odpw_portal_evolution.jinja",
            plot_script=script
            ,plot_div=div
            ,js_resources=js_resources
            ,css_resources=css_resources
            ,snapshot=snapshot
            , portalid=portalid
            , data=data
        )



@ui.route('/portal/<portalid>/<int:snapshot>/dist/formats', methods=['GET'])
def portalFormats(snapshot, portalid):
    with Timer(key="portalRes",verbose=True):
        db=current_app.config['dbc']

        Session=current_app.config['dbsession']
        data = getPortalInfos(Session,portalid,snapshot)
        data.update(aggregatePortalInfo(db,portalid,snapshot, limit=None))

        return render_template("odpw_portal_dist.jinja", data=data, snapshot=snapshot, portalid=portalid)

@ui.route('/portal/<portalid>/<int:snapshot>/dist/licenses', methods=['GET'])
def portalLicenses(snapshot, portalid):
    with Timer(key="portalRes",verbose=True):
        db=current_app.config['dbc']

        Session=current_app.config['dbsession']
        data = getPortalInfos(Session,portalid,snapshot)
        data.update(aggregatePortalInfo(db,portalid,snapshot, limit=None))

        return render_template("odpw_portal_dist.jinja", data=data, snapshot=snapshot, portalid=portalid)


@ui.route('/portal/<portalid>/<int:snapshot>/dist/organisations', methods=['GET'])
def portalOrganisations(snapshot, portalid):
    with Timer(key="portalRes",verbose=True):
        db=current_app.config['dbc']

        Session=current_app.config['dbsession']
        data = getPortalInfos(Session,portalid,snapshot)
        data.update(aggregatePortalInfo(db,portalid,snapshot, limit=None))

        return render_template("odpw_portal_dist.jinja", data=data, snapshot=snapshot, portalid=portalid)

def getPortalDatasets(Session, portalid,snapshot):
    return {"datasets": [ row2dict(r) for r in Session.query(Dataset.id).filter(Dataset.portalid==portalid).filter(Dataset.snapshot==snapshot).all()]}


@ui.route('/portal/<portalid>/<int:snapshot>/dataset', methods=['GET'], defaults={'dataset': None})
@ui.route('/portal/<portalid>/<int:snapshot>/dataset/<dataset>', methods=['GET'])
def portalDataset(snapshot, portalid, dataset):
    with Timer(key="portalDataset",verbose=True):
        Session=current_app.config['dbsession']
        data = getPortalInfos(Session,portalid,snapshot)

        data.update(getPortalDatasets(Session, portalid,snapshot))

        if dataset:
            r= Session.query(DatasetData).join(Dataset).filter(Dataset.id==dataset).join(DatasetQuality).add_entity(DatasetQuality).first()
            data['datasetData']=row2dict(r)
            data['json']=ast.literal_eval(data['datasetData']['raw'])


        return render_template("odpw_portal_dataset.jinja", data=data, snapshot=snapshot, portalid=portalid, dataset=dataset, qa=qa)


@ui.route('/portal/<portalid>/<int:snapshot>/quality', methods=['GET'])
def portalQuality(snapshot, portalid):
    with Timer(key="portalQuality",verbose=True):

        db=current_app.config['dbc']
        df=portalSnapshotQualityDF(db, portalid, snapshot)

        with Timer(key="dataDF", verbose=True) as t:
            p= qualityChart(df)

        script, div= components(p)

        js_resources = INLINE.render_js()
        css_resources = INLINE.render_css()

        Session=current_app.config['dbsession']
        data = getPortalInfos(Session,portalid,snapshot)
        return render_template("odpw_portal_quality.jinja",
            plot_script=script
            ,plot_div=div
            ,js_resources=js_resources
            ,css_resources=css_resources
            ,snapshot=snapshot
            , portalid=portalid
            , data=data
        )
