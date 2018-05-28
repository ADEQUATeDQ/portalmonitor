import ast
import datetime
import json
import time
from collections import defaultdict
from urlparse import urlparse

import jinja2
import pandas as pd
from bokeh.embed import components
from bokeh.resources import INLINE
from flask import Blueprint, current_app, render_template, jsonify
from markupsafe import Markup
from sqlalchemy import func, and_

from odpw.core.mat_views import withView, createView
from odpw.core.api import validURLDist, statusCodeDist, portalSnapshotQualityDF, getMetaResource, getResourceInfos
from odpw.utils.error_handling import errorStatus
from odpw.utils.plots import fetchProcessChart, qualityChart, qa, portalsScatter, evolutionCharts, \
    systemEvolutionPlot, portalDynamicity
from odpw.core.db import row2dict
from odpw.core.model import Portal, PortalSnapshotQuality, PortalSnapshot, Dataset, DatasetData, DatasetQuality, \
    MetaResource, ResourceInfo, PortalSnapshotDynamicity
from odpw.services.aggregates import aggregatePortalInfo
from odpw.utils.timing import Timer
from odpw.utils.utils_snapshot import getWeekString, getSnapshotfromTime, getPreviousWeek, getNextWeek, \
    getLastNSnapshots, getCurrentSnapshot
from odpw.web_rest.cache import cache
from odpw.reporter import dataset_reporter
from odpw.reporter.contact_reporter import orgaReport, contactPerOrga
from schemadotorg import dcat_to_schemadotorg

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


##-----------Helper Functions -----------##

def render(templateName, data=None,**kwargs):

    """
    FLask Jinja rendering function
    :param templateName: jinja template name
    :param data: json data for the template
    :return: html
    """

    portalCount=getPortalCount()
    #print portalCount
    #print kwargs
    if data is None:
        data={}
    #print data
    data['portalCount']=portalCount
    with Timer(key="renderHTML", verbose=True) as t:
        return render_template(templateName, data=data,**kwargs)


@cache.cached(timeout=300, key_prefix='getPortalCount')
def getPortalCount():

    with Timer(key="getPortalCount", verbose=True):
        return current_app.config['dbsession'].query(Portal.id).filter(Portal.active==True).count()

@ui.route('/', methods=['GET'])
def index():
    return render('index.jinja')

@ui.route('/about', methods=['GET'])
def about():
    return render('about.jinja')

@ui.route('/quality', methods=['GET'])
def qualitymetrics():
    return render('quality_metrics.jinja',qa=qa)

@ui.route('/sparql', methods=['GET'])
def sparqlendpoint():
    return render('sparql_endpoint.jinja')

@ui.route('/spec', methods=['GET'])
def spec():
    return render('spec.json', data={'host':"localhost:5123/", 'basePath':"api"})

@ui.route('/api', methods=['GET'])
def apispec():
    return render('apiui.jinja')

@ui.route('/timer', methods=['GET'])
def timer():
    print Timer.getStats()
    return render('timer.jinja', data={'stats':Timer.getStats()})

@ui.route('/system', methods=['GET'])
def system():
    with Timer(key="system" , verbose=True):
        return render("odpw_system_info.jinja")

@ui.route('/licensesearch', methods=['GET'])
@ui.route('/licensesearch/<path:uri>', methods=['GET'])
@cache.cached(timeout=60*60*24)
def licensesearch(uri=None):
    with Timer(key="get_licensesearch" , verbose=True):
        data={}
        if uri != None:
            cursn = getPreviousWeek(getSnapshotfromTime(datetime.datetime.now()))
            Session = current_app.config['dbsession']

            with Timer(key="query_licensesearch"):
                q = Session.query(Dataset, DatasetData) \
                    .join(MetaResource, Dataset.md5 == MetaResource.md5) \
                    .join(DatasetData, Dataset.md5 == DatasetData.md5) \
                    .filter(Dataset.snapshot == cursn) \
                    .filter(MetaResource.uri == uri)
                results=[]

                for r in q:
                    results.append(row2dict(r))

            data['uri'] = uri
            data['snapshot'] = cursn
            data['results']=results
        return render("odpw_license_search.jinja", data=data)

@ui.route('/system/changes', methods=['GET'])
@cache.cached(timeout=60*60*24)
def systemchanges():
    with Timer(key="get_systemchanges"):

        Session=current_app.config['dbsession']
        cursn=getSnapshotfromTime(datetime.datetime.now())
        prevWeek=getPreviousWeek(cursn)

        with Timer(key="query_systemchanges"):
            data_cur={r.portalid:r for r in Session.query(PortalSnapshot).filter(PortalSnapshot.snapshot==cursn)}
            data_prev={r.portalid:r for r in Session.query(PortalSnapshot).filter(PortalSnapshot.snapshot==prevWeek)}

        data={'status_change':{},'ds_change':{},'res_change':{}}
        for pid,ps in data_cur.items():
            if pid in data_prev:
                if ps.status == data_prev[pid].status:
                    if ps.datasetcount != data_prev[pid].datasetcount:
                        dsfrom=data_prev[pid].datasetcount if data_prev[pid].datasetcount is not None else 0
                        dsto=ps.datasetcount if ps.datasetcount  is not None else 0
                        data['ds_change'][pid]={'from':dsfrom , 'to':dsto}
                    elif ps.resourcecount != data_prev[pid].resourcecount:
                        resfrom=data_prev[pid].resourcecount if data_prev[pid].resourcecount is not None else 0
                        resto= ps.resourcecount if ps.resourcecount is not None else 0
                        data['res_change'][pid]={'from':resfrom, 'to':resto}
                else:
                    data['status_change'][pid]={'from':data_prev[pid].status, 'to':ps.status}

        data['from']=prevWeek
        data['to']=cursn

        return render("odpw_system_changes.jinja", data=data)

@ui.route('/system/fetch', methods=['GET'])
@cache.cached(timeout=60*60*24)
def systemfetch():
    with Timer(key="get_systemfetch"):
        Session=current_app.config['dbsession']

        cursn=getSnapshotfromTime(datetime.datetime.now())
        snapshots=getLastNSnapshots(cursn,n=5)
        nWeeksago=snapshots[-1]

        cnts=defaultdict(int)
        data={}
        with Timer(key="query_systemfetch"):
            for r in Session.query(PortalSnapshot.snapshot, PortalSnapshot.start, PortalSnapshot.end-PortalSnapshot.start).filter(PortalSnapshot.snapshot>nWeeksago):
                sn,start, dur = r[0], r[1],r[2]
                cnts[sn]+=1

                d=data.setdefault(sn,{})
                if dur is not None:
                    ds=d.setdefault(start,[])
                    ds.append(dur.total_seconds())

        for sn, d in data.items():
            dd=[]
            gstart= min(d.keys())

            for start, durations in d.items():
                for dur in durations:
                    delta=( start-gstart).total_seconds() + dur
                    dd.append(delta)
            data[sn]=dd

        with Timer(key="plot_systemfetch"):
            p= fetchProcessChart(data, cnts)
            script, div= components(p)

            js_resources = INLINE.render_js()
            css_resources = INLINE.render_css()

        return render("odpw_system_fetch.jinja",
            plot_script=script,
            plot_div=div,
            js_resources=js_resources,
            css_resources=css_resources
        )

@ui.route('/system/evolution', methods=['GET'])
@cache.cached(timeout=60*60*24)
def systemevolv():
    with Timer(key="get_systemevolv", verbose=True):
        Session=current_app.config['dbsession']

        with Timer(key="query_systemevolv", verbose=True):
            t= Session.query(PortalSnapshot.snapshot.label('snapshot'), Portal.software,PortalSnapshot.datasetcount,PortalSnapshot.resourcecount).join(Portal).subquery()
            q= Session.query(t.c.snapshot, t.c.software, func.count().label('count'),func.sum(t.c.resourcecount).label('resources'),func.sum(t.c.datasetcount).label('datasets')).group_by(t.c.snapshot,t.c.software)
            data=[ row2dict(r) for r in q.all()]
            df= pd.DataFrame(data)

        with Timer(key="plot_systemevolv", verbose=True):
            p=systemEvolutionPlot(df)
            script, div= components(p)

        js_resources = INLINE.render_js()
        css_resources = INLINE.render_css()

        return render("odpw_system_evolution.jinja",
            plot_script=script,
            plot_div=div,
            js_resources=js_resources,
            css_resources=css_resources
        )


#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
### PORTAL
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#

@cache.cached(timeout=60*60*24*7, key_prefix='getPortalsInfo')
def getPortalsInfo():

    with Timer(key="getPortalsInfo", verbose=True):
        ps=[]
        r=current_app.config['dbsession'].query(Portal, Portal.snapshot_count,Portal.first_snapshot, Portal.last_snapshot, Portal.datasetcount, Portal.resourcecount).filter(Portal.active==True)
        for P in r:
            #print 'P',P
            d={}
            d.update(row2dict(P[0]))
            d['snCount']=P[1]
            d['snFirst']=P[2]
            d['snLast']=P[3]
            d['datasets']=P[4]
            d['resources']=P[5]

            ps.append(d)
    return ps

@ui.route('/portalslist', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalslist():
    with Timer(key="get_portalslist", verbose=True):
        ps=getPortalsInfo()
        return render('odpw_portals.jinja', data={'portals':ps})

@ui.route('/portalstable', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalstable():
    with Timer(key="get_portalstable", verbose=True):
        ps=getPortalsInfo()
        return render('odpw_portals_table.jinja', data={'portals':ps})

@ui.route('/portals/portalsdynamic', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalsdynamicy():
    with Timer(key="get_portalsdynamicy", verbose=True):
        snapshot = getSnapshotfromTime(datetime.datetime.now())
        Session = current_app.config['dbsession']

        with Timer(key="query_portalsdynamicy", verbose=True):
            res = [r for r in
                   Session.query(Portal).join(PortalSnapshotDynamicity).filter(
                       PortalSnapshotDynamicity.snapshot == snapshot).add_entity(PortalSnapshotDynamicity)]
        results=[]
        keys = [
            'dindex',
            'changefrequ',
            'adddelratio',
            'dyratio',
            'staticRatio',
            'addRatio',
            'delRatio',
            'updatedRatio'
        ]
        for r in res:
            d=row2dict(r)
            for k in keys:
                d[k]=r[1].__getattribute__(k)

            results.append(d)

        df = pd.DataFrame(results)

        for c in keys:
            df[c] = df[c].convert_objects(convert_numeric=True)

        return render('odpw_portals_dynamics.jinja', data={'portals':results}, keys=keys, snapshot=snapshot)


@ui.route('/portals/portalsquality', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalsquality():
    with Timer(key="get_portalsquality", verbose=True):

        Session=current_app.config['dbsession']
        snapshot=getSnapshotfromTime(datetime.datetime.now())

        with Timer(key="query_portalsquality"):
            results=[row2dict(r) for r in Session.query(Portal, Portal.datasetcount, Portal.resourcecount).join(PortalSnapshotQuality).filter(PortalSnapshotQuality.snapshot==snapshot).add_entity(PortalSnapshotQuality)]

        keys=[ i.lower() for q in qa for i in q['metrics'] ]
        df=pd.DataFrame(results)

        #print df
        for c in keys:
            #print c,df[c]
            #print '___'*10
            df[c]=df[c].convert_objects(convert_numeric=True)

        dfiso= df.groupby(['iso'])
        dfiso=dfiso.agg('mean')\
             .join(pd.DataFrame(dfiso.size(),columns=['count']))
        resultsIso= dfiso.reset_index().to_dict(orient='records')


        dfsoft= df.groupby(['software'])
        dfsoft=dfsoft.agg('mean')\
             .join(pd.DataFrame(dfsoft.size(),columns=['count']))
        resultSoft= dfsoft.reset_index().to_dict(orient='records')

        return render('odpw_portals_quality.jinja', data={'portals':results,'iso':resultsIso,'soft':resultSoft}, keys=keys, snapshot=snapshot)

@ui.route('/portals/portalsstats', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalssize():
    with Timer(key="get_portalsstats", verbose=True):
        Session=current_app.config['dbsession']

        with Timer(key="query_portalsstats", verbose=True):
            results=[row2dict(r) for r in Session.query(Portal, Portal.snapshot_count,Portal.first_snapshot, Portal.last_snapshot, Portal.datasetcount, Portal.resourcecount)]
            df=pd.DataFrame(results)
        with Timer(key="plot_portalsstats", verbose=True):
            p= portalsScatter(df)
            script, div= components(p)

        js_resources = INLINE.render_js()
        css_resources = INLINE.render_css()

        return render("odpw_portals_stats.jinja",
            plot_script=script,
            plot_div=div,
            js_resources=js_resources,
            css_resources=css_resources
        )


#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
### PORTAL
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--#
@cache.cached(timeout=60*60*24)
def getPortalInfos(Session, portalid, snapshot):
    with Timer(key="get_getPortalInfos", verbose=True):
        with Timer(key="query_getPortalInfos", verbose=True):
            snapshots=[i[0] for i in Session.query(PortalSnapshot.snapshot).filter(PortalSnapshot.portalid==portalid).all()]

        p=getPreviousWeek(snapshot)
        p=p if p in snapshots else None
        n=getNextWeek(snapshot)
        n=n if n in snapshots else None
        data={'snapshots':{'list':snapshots,'prev':p, 'next': n}}
        return data

@ui.route('/portal', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portaldash():
    with Timer(key="get_portaldash", verbose=True):
        data={}
        cursn=getSnapshotfromTime(datetime.datetime.now())
        Session=current_app.config['dbsession']
        with Timer(key="query_portaldash", verbose=True):
            data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
        return render("odpw_portaldash.jinja",  data=data, snapshot=cursn)


def getResourceInfo(session,dbc, portalid, snapshot, orga=None):
    with Timer(key="getResourceInfo",verbose=True):
        data={}

        with Timer(key="query_getResourceInfoValid", verbose=True):
            data['valid']={}
            for valid in validURLDist(session,snapshot, portalid=portalid, orga=orga):
                data['valid'][valid[0]]=valid[1]
        with Timer(key="query_getResourceInfoStatus", verbose=True):
            data['status']={}
            if not orga:
                viewName = "view_{}_{}_{}".format('resstatus', portalid, snapshot)
            else:
                viewName = "view_{}_{}_{}_{}".format('resstatus', portalid, snapshot, orga)

            qorg = statusCodeDist(session,snapshot,portalid=portalid, orga=orga)
            q = withView(qorg, viewName, session, dbc)
            start = time.time()
            for res in q:
                data['status'][res[0]]=res[1]
            end = time.time()
            if (end - start) > 5:
                print("Create View {}".format(viewName))
                createView(qorg, viewName, session)

        return {'resourcesInfo':data}


# @ui.route('/portal/<portalid>/<int:snapshot>', methods=['GET'])
# @cache.cached(timeout=60*60*24)
# def portal(snapshot, portalid):
#     with Timer(key="get_portal",verbose=True):
#
#         Session=current_app.config['dbsession']
#         dbc=current_app.config['dbc']
#         data=getPortalInfos(Session,portalid,snapshot)
#
#         with Timer(key="query_portal",verbose=True):
#             r=Session.query(Portal, Portal.datasetcount, Portal.resourcecount).filter(Portal.id==portalid)
#             for P in r:
#                 data.update(row2dict(P[0]))
#                 data['datasets']=P[1]
#                 data['resources']=P[2]
#         with Timer(key="query_portal_agg", verbose=True):
#             data.update(aggregatePortalInfo(Session,portalid,snapshot,dbc))
#         return render("odpw_portal.jinja",  snapshot=snapshot, portalid=portalid,data=data)

@ui.route('/portal/<portalid>/', methods=['GET'])
@ui.route('/portal/<portalid>/<int:snapshot>', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portal(portalid,snapshot=getSnapshotfromTime(datetime.datetime.now())):
    with Timer(key="get_portal",verbose=True):
        current_sn = snapshot
        Session=current_app.config['dbsession']
        data=getPortalInfos(Session,portalid,snapshot)
        dynamicityEnabled = current_app.config.get('dynamicity', False)


        with Timer(key="query_portal",verbose=True):
            q = Session.query(Portal).filter(Portal.id == portalid) \
                .join(PortalSnapshotQuality, PortalSnapshotQuality.portalid == Portal.id) \
                .filter(PortalSnapshotQuality.snapshot == snapshot) \
                .join(PortalSnapshot, PortalSnapshot.portalid == Portal.id) \
                .filter(PortalSnapshot.snapshot == snapshot) \
                .add_entity(PortalSnapshot) \
                .add_entity(PortalSnapshotQuality)

            if dynamicityEnabled:
                q = q.join(PortalSnapshotDynamicity, PortalSnapshotDynamicity.portalid == Portal.id) \
                    .filter(PortalSnapshotDynamicity.snapshot == snapshot) \
                    .add_entity(PortalSnapshotDynamicity)
            r = q.first()
            while r is None:
                snapshot= getPreviousWeek(snapshot)
                q = Session.query(Portal).filter(Portal.id == portalid) \
                    .join(PortalSnapshotQuality, PortalSnapshotQuality.portalid == Portal.id) \
                    .filter(PortalSnapshotQuality.snapshot == snapshot) \
                    .join(PortalSnapshot, PortalSnapshot.portalid == Portal.id) \
                    .filter(PortalSnapshot.snapshot == snapshot) \
                    .add_entity(PortalSnapshot) \
                    .add_entity(PortalSnapshotQuality)

                if dynamicityEnabled:
                    q = q.join(PortalSnapshotDynamicity, PortalSnapshotDynamicity.portalid == Portal.id) \
                        .filter(PortalSnapshotDynamicity.snapshot == snapshot) \
                        .add_entity(PortalSnapshotDynamicity)
                r = q.first()

            data['portal'] = row2dict(r[0])
            data['fetchInfo'] = row2dict(r[1])
            data['fetchInfo']['duration']=data['fetchInfo']['end']-data['fetchInfo']['start']

            if dynamicityEnabled:
                data['dynamicity'] = row2dict(r[3])
            data['quality'] = row2dict(r[2])


        #with Timer(key="query_portal_agg", verbose=True):
        #    data.update(aggregatePortalInfo(Session,portalid,snapshot,dbc))
        return render("odpw_portal.jinja",  snapshot=current_sn, portalid=portalid,data=data)

@ui.route('/portal/<portalid>/<int:snapshot>/report', methods=['GET'])
#@cache.cached(timeout=60*60*24)
def portalreport(portalid,snapshot=getSnapshotfromTime(datetime.datetime.now())):
    with Timer(key="get_portal",verbose=True):

        Session=current_app.config['dbsession']
        data=getPortalInfos(Session,portalid,snapshot)
        with Timer(key="query_portalreport",verbose=True):
            q = Session.query(Dataset.organisation) \
                .filter(Dataset.portalid == portalid) \
                .filter(Dataset.snapshot == snapshot).distinct(Dataset.organisation)

            data['organisations']=[ row2dict(res) for res in q]
        return render("odpw_portal_report.jinja",  snapshot=snapshot, portalid=portalid,data=data)

@ui.route('/portal/<portalid>/<int:snapshot>/report/<orga>', methods=['GET'])
#@cache.cached(timeout=60*60*24)
def portalOrgareport(portalid,orga,snapshot=getSnapshotfromTime(datetime.datetime.now())):
    with Timer(key="get_portal",verbose=True):

        Session=current_app.config['dbsession']
        data=getPortalInfos(Session,portalid,snapshot)

        with Timer(key="query_portalreport",verbose=True):
            portal=Session.query(Portal).filter(Portal.id==portalid).first()
            data['contacts']=contactPerOrga(Session, portal, snapshot, orga)

        return render("odpw_portal_report_contacts.jinja",  snapshot=snapshot, portalid=portalid,data=data,organisation=orga)


@ui.route('/portal/<portalid>/<int:snapshot>/report/<contact>/<orga>', methods=['GET'])
def portalOrgareportContact(portalid,contact,orga,snapshot=getSnapshotfromTime(datetime.datetime.now())):
    print contact
    Session = current_app.config['dbsession']
    data = getPortalInfos(Session, portalid, snapshot)

    with Timer(key="query_portalreport", verbose=True):
        portal = Session.query(Portal).filter(Portal.id == portalid).first()
        data['contact_report'] = orgaReport(Session, portal, snapshot, orga, contact=contact)

    return render("odpw_portal_report_contact.jinja", snapshot=snapshot, portalid=portalid, data=data,
                  organisation=orga, contact=contact)


@ui.route('/portal/<portalid>/<int:snapshot>/resource/<path:uri>', methods=['GET'])
@cache.cached(timeout=60*60*24)
def resourceInfo(snapshot, portalid, uri):
    with Timer(key="get_resourceInfo",verbose=True):
        #print snapshot,portalid,uri

        Session=current_app.config['dbsession']
        dbc = current_app.config['dbc']
        data=getPortalInfos(Session, portalid, snapshot)


        with Timer(key="query_resources",verbose=True):
            viewName = "view_{}_{}_{}".format('resinfo', portalid, snapshot)
            qorg = getResourceInfos(Session, snapshot, portalid)
            q = withView(qorg, viewName, Session, dbc)
            start = time.time()
            data['resources'] = [row2dict(r[0]) for r in q.all()]
            end = time.time()
            if (end - start) > 5:
                print("Create View {}".format(viewName))
                createView(qorg, viewName, Session)

        with Timer(key="query_resourceInfo", verbose=True):
            q = Session.query(ResourceInfo) \
                .filter(ResourceInfo.uri == uri)
            #print q
            data['resourceInfo'] = [row2dict(r) for r in q.all()]

            for r in data['resourceInfo']:
                if 'header' in r:
                    if r['header'] is None:
                        r['header']=""
                    else:
                        #print type(r['header']),r['header'],r
                        r['header'] = ast.literal_eval(str(r['header']))


        return render("odpw_portal_resource.jinja", snapshot=snapshot, portalid=portalid, uri=uri, data=data)


@ui.route('/portal/<portalid>/<int:snapshot>/linkcheck/', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalLinkCheck(snapshot, portalid):
    Session = current_app.config['dbsession']

    data = getPortalInfos(Session, portalid, snapshot)
    q = Session.query(Dataset.organisation) \
        .filter(Dataset.portalid == portalid) \
        .filter(Dataset.snapshot == snapshot).distinct(Dataset.organisation)

    data['organisations'] = [row2dict(res) for res in q]

    return render("odpw_portal_linkchecker.jinja", snapshot=snapshot, portalid=portalid, data=data)


@ui.route('/portal/<portalid>/<int:snapshot>/resources', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalRes(portalid, snapshot=None):
    if not snapshot:
        snapshot = getCurrentSnapshot()
    Session = current_app.config['dbsession']
    data={}
    data.update(getPortalInfos(Session, portalid, snapshot))
    return render("odpw_portal_resources.jinja",  data=data,snapshot=snapshot, portalid=portalid)


def getDatasetURI(datasetid, portalid):
    session=current_app.config['dbsession']
    p = session.query(Portal).filter(Portal.id == portalid).first()
    if p.software == 'CKAN':
        uri = '{0}/dataset/{1}'.format(p.apiuri.rstrip('/'), datasetid)
    elif p.software == 'Socrata':
        uri = '{0}/dataset/{1}'.format(p.uri.rstrip('/'), datasetid)
    elif p.software == 'OpenDataSoft':
        uri = '{0}/explore/dataset/{1}'.format(p.uri.rstrip('/'), datasetid)
    else:
        uri = datasetid
    return uri


@ui.route('/portal/<portalid>/<int:snapshot>/linkcheck/<orga>', methods=['GET'])
@cache.cached(timeout=60*60*24)
def orga_resources(portalid, snapshot, orga):
    Session = current_app.config['dbsession']
    data = {}
    data.update(getPortalInfos(Session, portalid, snapshot))
    return render("odpw_portal_linkchecker_orga.jinja", data=data, snapshot=snapshot, portalid=portalid, organisation=orga)


@ui.route('/portal/<portalid>/<int:snapshot>/linkcheck/<orga>/body', methods=['GET'])
@cache.cached(timeout=60*60*24)
def orga_resource(portalid, snapshot, orga):
    with Timer(key="get_orga_resource",verbose=True):
        Session=current_app.config['dbsession']
        dbc=current_app.config['dbc']

        data = getResourceInfo(Session, dbc, portalid, snapshot, orga)
        q = getResourceInfos(Session, snapshot, portalid, orga)

        data['resList'] = []
        for i in q:
            dataset = i[1]
            orig_link = getDatasetURI(dataset.id, portalid)
            data['resList'].append({'uri': row2dict(i[0]), 'dataset': {'uri': orig_link, 'title': dataset.title}})

        data.update(getPortalInfos(Session,portalid,snapshot))
        r=current_app.config['dbsession'].query(Portal.resourcecount).filter(Portal.id==portalid)
        for P in r:
            data['resources']=P[0]

        return render("odpw_portal_resources_list.jinja", data=data, snapshot=snapshot, portalid=portalid)


@ui.route('/portal/<portalid>/<int:snapshot>/resources/body', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalResBody(snapshot, portalid):
    with Timer(key="get_portalRes",verbose=True):
        Session=current_app.config['dbsession']
        dbc=current_app.config['dbc']
        with Timer(key="query_portalRes", verbose=True):
            data=getResourceInfo(Session,dbc, portalid, snapshot)


        with  Timer(key="query_getMetaResource", verbose=True):
            viewName = "view_{}_{}_{}".format('resinfo', portalid, snapshot)
            qorg = getResourceInfos(Session,snapshot, portalid)
            q = withView(qorg, viewName, Session, dbc)


            start = time.time()
            data['resList'] = []
            for i in q:
                dataset = i[1]
                orig_link = getDatasetURI(dataset.id, portalid)
                data['resList'].append({'uri': row2dict(i[0]), 'dataset': {'uri': orig_link, 'title': dataset.title}})
            end = time.time()
            if (end - start) > 5:
                print("Create View {}".format(viewName))
                createView(qorg, viewName, Session)

        data.update(getPortalInfos(Session,portalid,snapshot))
        #data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
        with Timer(key="query_portalResourceCount", verbose=True):
            r=current_app.config['dbsession'].query(Portal.resourcecount).filter(Portal.id==portalid)
            ps=[]
            for P in r:
                data['resources']=P[0]

        return render("odpw_portal_resources_list.jinja", data=data, snapshot=snapshot, portalid=portalid)



@ui.route('/portal/<portalid>/<int:snapshot>/evolution', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalEvolution(snapshot, portalid):
    with Timer(key="get_portalEvolution",verbose=True):
        Session=current_app.config['dbsession']
        data={}
        with Timer(key="query_portalEvolution", verbose=True):
            for R in Session.query(PortalSnapshot).filter(PortalSnapshot.portalid==portalid):
                data[R.portalid+str(R.snapshot)]=row2dict(R)
            for R in Session.query(PortalSnapshotQuality).filter(PortalSnapshotQuality.portalid==portalid):
                data[R.portalid+str(R.snapshot)].update(row2dict(R))

        df=pd.DataFrame([v for k,v in data.items()])
        with Timer(key="plot_portalEvolution", verbose=True):
            p=evolutionCharts(df)
            script, div= components(p)

            js_resources = INLINE.render_js()
            css_resources = INLINE.render_css()

        data = getPortalInfos(Session,portalid,snapshot)

        return render("odpw_portal_evolution.jinja",
            plot_script=script
            ,plot_div=div
            ,js_resources=js_resources
            ,css_resources=css_resources
            ,snapshot=snapshot
            , portalid=portalid
            , data=data
        )



@ui.route('/portal/<portalid>/<int:snapshot>/dist/formats', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalFormats(snapshot, portalid):
    with Timer(key="get_portalFormatDist",verbose=True):
        Session=current_app.config['dbsession']
        dbc = current_app.config['dbc']
        data = getPortalInfos(Session, portalid, snapshot)

        with Timer(key="query_portalFormatDist", verbose=True):
            data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
            data.update(aggregatePortalInfo(Session,portalid,snapshot, dbc, limit=None))

        return render("odpw_portal_dist.jinja", data=data, snapshot=snapshot, portalid=portalid)

@ui.route('/portal/<portalid>/<int:snapshot>/dist/licenses', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalLicenses(snapshot, portalid):
    with Timer(key="get_portalLicenseDist",verbose=True):
        Session=current_app.config['dbsession']
        dbc=current_app.config['dbc']
        data = getPortalInfos(Session,portalid,snapshot)
        with Timer(key="query_portalLicenseDist", verbose=True):
            data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
        data.update(aggregatePortalInfo(Session,portalid,snapshot, dbc, limit=None))

        return render("odpw_portal_dist.jinja", data=data, snapshot=snapshot, portalid=portalid)


@ui.route('/portal/<portalid>/<int:snapshot>/dist/organisations', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalOrganisations(snapshot, portalid):
    with Timer(key="portalRes",verbose=True):
        Session=current_app.config['dbsession']
        dbc = current_app.config['dbc']
        data = getPortalInfos(Session,portalid,snapshot)
        data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
        data.update(aggregatePortalInfo(Session,portalid,snapshot, dbc,limit=None))

        return render("odpw_portal_dist.jinja", data=data, snapshot=snapshot, portalid=portalid)

def getPortalDatasets(Session, portalid,snapshot):
    with Timer(key="getPortalDatasets",verbose=True):
        return {"datasets": [ row2dict(r) for r in Session.query(Dataset.title, Dataset.id).filter(Dataset.portalid==portalid).filter(Dataset.snapshot==snapshot).all()]}


@ui.route('/portal/<portalid>/<int:snapshot>/dataset', methods=['GET'], defaults={'dataset': None})
@ui.route('/portal/<portalid>/dataset/<path:dataset>', methods=['GET'], defaults={'snapshot': None})
@ui.route('/portal/<portalid>/<int:snapshot>/dataset/<path:dataset>', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalDataset(snapshot, portalid, dataset):
    with Timer(key="portalDataset",verbose=True):

        if not snapshot:
            snapshot = getCurrentSnapshot()

        Session=current_app.config['dbsession']
        data = getPortalInfos(Session,portalid,snapshot)
        #data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
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
                software = Session.query(Portal.software).filter(Portal.id==portalid).first()[0]
                if software == 'Socrata':
                    data['json']=data['datasetData']['raw']['view']
                else:
                    data['json']=data['datasetData']['raw']
                data['report']=dataset_reporter.report(r[0],r[1], software=None)

                #with Timer(key="getSchemadotorgDatasets", verbose=True):
                #    q = Session.query(Portal).filter(Portal.id == portalid)
                #    p = q.first()
                #    schemadotorg = json.dumps(dcat_to_schemadotorg.convert(p, r[0]), indent=3)

            with Timer(key="getPortalDatasets_resources",verbose=True):
                q= Session.query(MetaResource,ResourceInfo).filter(MetaResource.md5==r[0].md5).outerjoin(ResourceInfo, and_( ResourceInfo.uri==MetaResource.uri,ResourceInfo.snapshot==snapshot))
                data['resources']=[row2dict(r) for r in q.all()]
                for r in data['resources']:
                    if 'header' in r and isinstance(r['header'], basestring):
                        r['header']=ast.literal_eval(r['header'])


        with Timer(key="getPortalDatasets_versions",verbose=True):
            q=Session.query(Dataset.md5, func.min(Dataset.snapshot).label('min'), func.max(Dataset.snapshot).label('max')).filter(Dataset.id==dataset).group_by(Dataset.md5)
            r=[row2dict(r) for r in q.all()]
            print r
            versions={}
            for i in r:
                a=versions.setdefault(i['md5'],[])
                a.append({'min':i['min'],'max':i['max']})
            data['versions']=r


        return render("odpw_portal_dataset.jinja", data=data, snapshot=snapshot, portalid=portalid, dataset=dd, qa=qa, error=errorStatus)


@ui.route('/portal/<portalid>/<int:snapshot>/csvw', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalCSVW(snapshot, portalid):
    with Timer(key="portalCSVW",verbose=True):
        Session=current_app.config['dbsession']
        data = getPortalInfos(Session,portalid,snapshot)
        data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
        data.update(getPortalDatasets(Session, portalid, snapshot))

        q = Session.query(Dataset, MetaResource)\
            .filter(MetaResource.md5==Dataset.md5)\
            .filter(MetaResource.format=='csv')\
            .filter(Dataset.portalid==portalid)\
            .filter(Dataset.snapshot==snapshot)

        data['resources']=[row2dict(r) for r in q.all()]

        return render("odpw_portal_csvw.jinja", data=data, snapshot=snapshot, portalid=portalid, qa=qa, error=errorStatus)



@ui.route('/portal/<portalid>/<int:snapshot>/quality', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalQuality(snapshot, portalid):
    with Timer(key="portalQuality",verbose=True):

        Session = current_app.config['dbsession']
        df=portalSnapshotQualityDF(Session, portalid, snapshot)
        q = Session.query(PortalSnapshotQuality) \
            .filter(PortalSnapshotQuality.portalid == portalid) \
            .filter(PortalSnapshotQuality.snapshot == snapshot)
        qdata = None
        for r in q:
            qdata = row2dict(r)
            break
        d = []

        datasets = int(qdata['datasets'])
        for inD in qa:
            for k, v in inD['metrics'].items():
                k = k.lower()
                # TODO what to do if metric has no value?
                if qdata[k] != None and qdata[k] != 'None':
                    value = float(qdata[k])
                    perc = int(qdata[k + 'N']) / (datasets * 1.0) if datasets > 0 else 0
                    c = {'Metric': k, 'Dimension': inD['dimension'],
                         'dim_color': inD['color'], 'value': value, 'perc': perc}
                    c.update(v)
                    d.append(c)

        data = getPortalInfos(Session,portalid,snapshot)
        js_resources = INLINE.render_js()
        css_resources = INLINE.render_css()

        if d:
            df= pd.DataFrame(d)
            with Timer(key="dataDF", verbose=True) as t:
                p= qualityChart(df)

            script, div= components(p)


            data['portals']= [ row2dict(r) for r in Session.query(Portal).all()]
            data['quality']=qdata
            return render("odpw_portal_quality.jinja",
                plot_script=script
                ,plot_div=div
                ,js_resources=js_resources
                ,css_resources=css_resources
                ,snapshot=snapshot
                , portalid=portalid
                , data=data
                , qa=qa
            )
        else:
            return render("odpw_portal_quality.jinja", snapshot=snapshot, js_resources=js_resources, css_resources=css_resources, portalid=portalid, data=data, qa=qa)


@ui.route('/portal/<portalid>/<int:snapshot>/dynamics', methods=['GET'])
@cache.cached(timeout=60*60*24)
def portalDynamicy(snapshot, portalid):
    Session = current_app.config['dbsession']
    q= Session.query(PortalSnapshotDynamicity).filter(PortalSnapshotDynamicity.portalid==portalid).filter(PortalSnapshotDynamicity.snapshot<=snapshot)

    data=[]
    keys = [
        'dindex',
        'changefrequ',
        'adddelratio',
        'dyratio',
        'staticRatio',
        'addRatio',
        'delRatio',
        'updatedRatio'
    ]
    for psd in q:
        d = row2dict(psd)
        for k in keys:
            d[k] = psd.__getattribute__(k)
        data.append(d)

    df = pd.DataFrame(data)
    with Timer(key="dynPlot", verbose=True) as t:
        p = portalDynamicity(df)

    script, div = components(p)

    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()

    data = getPortalInfos(Session, portalid, snapshot)
    data['portals'] = [row2dict(r) for r in Session.query(Portal).all()]
    return render("odpw_portal_dynamicity.jinja",
                  plot_script=script
                  , plot_div=div
                  , js_resources=js_resources
                  , css_resources=css_resources
                  , snapshot=snapshot
                  , portalid=portalid
                  , data=data
                  )
