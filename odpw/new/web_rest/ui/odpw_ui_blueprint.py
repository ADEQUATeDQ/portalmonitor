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
from sqlalchemy import func, and_

from odpw.new.core.api import validURLDist, statusCodeDist
from odpw.new.utils.error_handling import errorStatus
from odpw.new.utils.plots import fetchProcessChart, qualityChart, qa, portalsScatter, evolutionCharts, \
    systemEvolutionPlot
from odpw.new.core.db import row2dict
from odpw.new.core.model import Portal, PortalSnapshotQuality, PortalSnapshot, Dataset, DatasetData, DatasetQuality, \
    MetaResource, ResourceInfo
from odpw.new.services.aggregates import aggregatePortalInfo
from odpw.new.utils.timing import Timer
from odpw.new.utils.utils_snapshot import getWeekString, getSnapshotfromTime, getPreviousWeek, getNextWeek
from odpw.new.web_rest.cache import cache

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

@ui.route('/about', methods=['GET'])
def about():
    return render_template('about.jinja')

@ui.route('/quality', methods=['GET'])
def qualitymetrics():
    return render_template('quality_metrics.jinja',qa=qa)

@ui.route('/spec', methods=['GET'])
def spec():
    return render_template('spec.json', host="localhost:5122/", basePath="api")

@ui.route('/api', methods=['GET'])
def api():
    return render_template('apiui.jinja')

@ui.route('/timer', methods=['GET'])
def timer():
    print Timer.getStats()
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
@ui.route('/system/evolution', methods=['GET'])
def systemevolv():
    with Timer(key="systemevolv"):
        db=current_app.config['dbc']

        t= db.Session.query(PortalSnapshot.snapshot.label('snapshot'), Portal.software,PortalSnapshot.datasetCount,PortalSnapshot.resourceCount).join(Portal).subquery()
        q=db.Session.query(t.c.snapshot, t.c.software, func.count().label('count'),func.sum(t.c.resourceCount).label('resources'),func.sum(t.c.datasetCount).label('datasets')).group_by(t.c.snapshot,t.c.software)
        data=[ row2dict(r) for r in q.all()]

        #data=[ row2dict(r) for r in db.Session.query(t.c.snapshot, t.c.software, func.count().label('count')).group_by(t.c.snapshot,t.c.software).all()]

        df= pd.DataFrame(data)

        p=systemEvolutionPlot(df)
        script, div= components(p)
        print div

        js_resources = INLINE.render_js()
        css_resources = INLINE.render_css()

        return render_template("odpw_system_evolution.jinja",
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
        db=current_app.config['dbc']



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
        results=[row2dict(r) for r in db.Session.query(Portal, Portal.datasetCount, Portal.resourceCount).join(PortalSnapshotQuality).filter(PortalSnapshotQuality.snapshot==snapshot).add_entity(PortalSnapshotQuality)]

        keys=[ i.lower() for q in qa for i in q['metrics'] ]
        df=pd.DataFrame(results)
        print df
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
        return data

@ui.route('/portal', methods=['GET'])
def portaldash():
    data={}
    cursn=getSnapshotfromTime(datetime.datetime.now())
    Session=current_app.config['dbsession']
    data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
    return render_template("odpw_portaldash.jinja",  data=data, snapshot=cursn)


def getResourceInfo(session, portalid, snapshot):

    with Timer(key="getResourceInfo",verbose=True):
        data={}

        data['valid']={}
        for valid in validURLDist(session,snapshot, portalid=portalid):
            data['valid'][valid[0]]=valid[1]
        data['status']={}
        for res in statusCodeDist(session,snapshot,portalid=portalid):
            data['status'][res[0]]=res[1]

        return {'resourcesInfo':data}





@ui.route('/portal/<portalid>/<int:snapshot>', methods=['GET'])
def portal(snapshot, portalid):
    with Timer(key="portal",verbose=True):

        Session=current_app.config['dbsession']
        data=getPortalInfos(Session,portalid,snapshot)
        data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]

        with Timer(key="portalQuery",verbose=True):
            r=current_app.config['dbsession'].query(Portal, Portal.datasetCount, Portal.resourceCount).filter(Portal.id==portalid)
            ps=[]
            for P in r:
                data.update(row2dict(P[0]))
                data['datasets']=P[1]
                data['resources']=P[2]

        data.update(aggregatePortalInfo(Session,portalid,snapshot))


        return render_template("odpw_portal.jinja",  snapshot=snapshot, portalid=portalid,data=data)


@ui.route('/portal/<portalid>/<int:snapshot>/resources', methods=['GET'])
def portalRes(snapshot, portalid):
    with Timer(key="portalRes",verbose=True):
        db=current_app.config['dbc']
        Session=current_app.config['dbsession']

        data=getResourceInfo(Session, portalid, snapshot)

        #data['uris']= [row2dict(i) for i in db.getResourceInfos(snapshot,portalid=portalid) ]

        data.update(getPortalInfos(Session,portalid,snapshot))
        data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
        with Timer(key="portalQuery",verbose=True):
            r=current_app.config['dbsession'].query(Portal.resourceCount).filter(Portal.id==portalid)
            ps=[]
            for P in r:
                data['resources']=P[0]

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
        data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
        data.update(aggregatePortalInfo(Session,portalid,snapshot, limit=None))

        return render_template("odpw_portal_dist.jinja", data=data, snapshot=snapshot, portalid=portalid)

@ui.route('/portal/<portalid>/<int:snapshot>/dist/licenses', methods=['GET'])
def portalLicenses(snapshot, portalid):
    with Timer(key="portalRes",verbose=True):
        db=current_app.config['dbc']

        Session=current_app.config['dbsession']
        data = getPortalInfos(Session,portalid,snapshot)
        data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
        data.update(aggregatePortalInfo(Session,portalid,snapshot, limit=None))

        return render_template("odpw_portal_dist.jinja", data=data, snapshot=snapshot, portalid=portalid)


@ui.route('/portal/<portalid>/<int:snapshot>/dist/organisations', methods=['GET'])
def portalOrganisations(snapshot, portalid):
    with Timer(key="portalRes",verbose=True):
        db=current_app.config['dbc']

        Session=current_app.config['dbsession']
        data = getPortalInfos(Session,portalid,snapshot)
        data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
        data.update(aggregatePortalInfo(Session,portalid,snapshot, limit=None))

        return render_template("odpw_portal_dist.jinja", data=data, snapshot=snapshot, portalid=portalid)

def getPortalDatasets(Session, portalid,snapshot):
    with Timer(key="getPortalDatasets",verbose=True):
        return {"datasets": [ row2dict(r) for r in Session.query(Dataset.title, Dataset.id).filter(Dataset.portalid==portalid).filter(Dataset.snapshot==snapshot).all()]}


@ui.route('/portal/<portalid>/<int:snapshot>/dataset', methods=['GET'], defaults={'dataset': None})
@ui.route('/portal/<portalid>/<int:snapshot>/dataset/<dataset>', methods=['GET'])
def portalDataset(snapshot, portalid, dataset):
    with Timer(key="portalDataset",verbose=True):
        Session=current_app.config['dbsession']
        data = getPortalInfos(Session,portalid,snapshot)
        data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
        data.update(getPortalDatasets(Session, portalid, snapshot))

        dd=None
        if dataset:
            for dt in data['datasets']:
                if dt['id']==dataset:
                    dd=dt
                    break
            with Timer(key="getPortalDatasets_datasetData",verbose=True):
                r= Session.query(DatasetData).join(Dataset).filter(Dataset.id==dataset).join(DatasetQuality).add_entity(DatasetQuality).first()
                data['datasetData']=row2dict(r)
                data['json']=ast.literal_eval(data['datasetData']['raw'])

            with Timer(key="getPortalDatasets_resources",verbose=True):
                q= Session.query(MetaResource,ResourceInfo).filter(MetaResource.md5==r[0].md5).outerjoin(ResourceInfo, and_( ResourceInfo.uri==MetaResource.uri,ResourceInfo.snapshot==snapshot))
                data['resources']=[row2dict(r) for r in q.all()]
                for r in data['resources']:
                    print r
                    if 'header' in r:
                        print r['header']
                        r['header']=ast.literal_eval(r['header'])
                        print r['header']

        with Timer(key="getPortalDatasets_versions",verbose=True):
            q=Session.query(Dataset.md5, func.min(Dataset.snapshot).label('min'), func.max(Dataset.snapshot).label('max')).filter(Dataset.id==dataset).group_by(Dataset.md5)
            print q
            data['versions']=[row2dict(r) for r in q.all()]
            print data['versions']

        return render_template("odpw_portal_dataset.jinja", data=data, snapshot=snapshot, portalid=portalid, dataset=dd, qa=qa, error=errorStatus)


@ui.route('/portal/<portalid>/<int:snapshot>/quality', methods=['GET'])
def portalQuality(snapshot, portalid):
    with Timer(key="portalQuality",verbose=True):

        db=current_app.config['dbc']
        df=db.portalSnapshotQualityDF( portalid, snapshot)

        with Timer(key="dataDF", verbose=True) as t:
            p= qualityChart(df)

        script, div= components(p)

        js_resources = INLINE.render_js()
        css_resources = INLINE.render_css()

        Session=current_app.config['dbsession']
        data = getPortalInfos(Session,portalid,snapshot)
        data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
        return render_template("odpw_portal_quality.jinja",
            plot_script=script
            ,plot_div=div
            ,js_resources=js_resources
            ,css_resources=css_resources
            ,snapshot=snapshot
            , portalid=portalid
            , data=data
        )
