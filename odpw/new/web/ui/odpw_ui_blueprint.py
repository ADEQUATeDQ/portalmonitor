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
