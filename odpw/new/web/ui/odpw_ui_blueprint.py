import StringIO

import csv
import json
from functools import wraps
from urlparse import urlparse

import jinja2
from sqlalchemy.util._collections import KeyedTuple

from flask import Blueprint, current_app, jsonify, make_response, render_template, Response
from sqlalchemy import inspect

from odpw.new.utils_snapshot import getWeekString
from odpw.new.web.rest.odpw_restapi_blueprint import row2dict
from odpw.new.web.cache import cache
from odpw.new.model import Portal, PortalSnapshotQuality, PortalSnapshot, Base

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


@ui.route('/portals', methods=['GET'])
def portals():


    r=current_app.config['dbsession'].query(Portal, Portal.snapshot_count,Portal.first_snapshot, Portal.last_snapshot, Portal.datasetCount, Portal.resourceCount)
    ps=[]
    for P in r:
        d={}
        d.update(row2dict(P[0]))
        d['snCount']=P[1]
        d['snFirst']=P[2]
        d['snLast']=P[3]
        d['datasets']=P[4]
        d['resources']=P[5]
        print d
        ps.append(d)
    return render_template('odpw_portals.jinja', data=ps)

