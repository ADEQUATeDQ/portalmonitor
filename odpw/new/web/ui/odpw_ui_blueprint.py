import StringIO

import csv
import json
from functools import wraps

from sqlalchemy.util._collections import KeyedTuple

from flask import Blueprint, current_app, jsonify, make_response, render_template, Response
from sqlalchemy import inspect

from odpw.new.web.cache import cache
from odpw.new.model import Portal, PortalSnapshotQuality, PortalSnapshot, Base

ui = Blueprint('ui', __name__,
                    template_folder='../templates',
                    static_folder='../static',
                    )



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


    r=current_app.config['dbsession'].query(Portal, Portal.snapshot_count,Portal.first_snapshot, Portal.last_snapshot, Portal.datasetCount)
    for P in r:
        print P
    return render_template('odpw_portals.jinja')

