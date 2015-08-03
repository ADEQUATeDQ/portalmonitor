#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tornado.web import RequestHandler, HTTPError
from jinja2.exceptions import TemplateNotFound
from tornado.escape import json_encode

from odpw.db.models import Portal
from odpw.utils  import util
from collections import defaultdict
from urlparse import urlparse 
import json
from odpw.db.dbm import nested_json, date_handler
from odpw.reporting.reporters import DBAnalyser, DFtoListDict, addPercentageCol,\
    ReporterEngine, ISO3DistReporter, SoftWareDistReporter,\
    SystemActivityReporter, SnapshotsPerPortalReporter
from odpw.utils.timer import Timer


class BaseHandler(RequestHandler):
    @property
    def env(self):
        return self.application.env

    @property
    def db(self):
        return self.application.db

    def get_error_html(self, status_code, **kwargs):
        try:
            self.render('error/{}.html'.format(status_code))
        except (TemplateNotFound, HTTPError):
            try:
                self.render('error/50x.html', status_code=status_code)
            except (TemplateNotFound, HTTPError):
                self.write("You aren't supposed to see this")

    def render(self, template, **kwargs):
        try:
            template = self.env.get_template(template)
        except TemplateNotFound:
            raise HTTPError(404)
        self.env.globals['static_url'] = self.static_url
        
        with open('index_rend.html', "w") as f:
            f.write(template.render(kwargs ))
        
        self.write(template.render(kwargs))


class NoDestinationHandler(RequestHandler):
    def get(self):
        raise HTTPError(503)

class PortalHandler(BaseHandler):
    def get(self, **params):
        print params
        
        
        
        if not bool(params):
            rep = ReporterEngine([SnapshotsPerPortalReporter(self.db)])
            rep.run()
            self.render('portal_detail_empty.jinja',portals=True, data=rep.uireport())
        elif params['portal']:
            if len(params['portal'].split(",")) ==1:
                pid=params['portal']
                
                rep = ReporterEngine([SnapshotsPerPortalReporter(self.db)])
                
                rep.run()
                #r = PortalOverviewReporter(self.db, portalID=pid)
                
                
                self.render('portal_detail.jinja',portals=True)
            else:
                self.render('portals_detail.jinja',portals=True)
            
                
        
        
class PortalList(BaseHandler):
    def get(self):
        #with Timer(verbose=True) as t:
            try:
                p={}
                for por in self.db.getPortals():
                    p[por['id']]={'iso3':por['iso3'],'software':por['software']}
                portals=[]
                for pRes in self.db.getLatestPortalMetaDatas():
                    rdict= dict(pRes)
                    if rdict['portal_id'] in p:
                        rdict['iso3']=p[rdict['portal_id']]['iso3']
                        rdict['software']=p[rdict['portal_id']]['software'] 
            
                    portals.append(rdict)
        
                self.render('portallist.jinja',index=True, data=portals, json=json.dumps(portals, default=date_handler))
            except Exception as e:print e

class IndexHandler(BaseHandler):
    def get(self):
        
        sys_or = ReporterEngine([SoftWareDistReporter(self.db),
                 ISO3DistReporter(self.db)])
        sys_or.run()
            
        self.render('index.html',index=True, json=json.dumps(sys_or.uireport(), default=date_handler))
        
class SystemActivityHandler(BaseHandler):
    def get(self):
        
        sn = util.getCurrentSnapshot()
        sn=1531
        sys_act_rep = ReporterEngine([SystemActivityReporter(self.db,sn)])
        sys_act_rep.run()
        
        self.render('system_activity.jinja',index=True, json=json.dumps(sys_act_rep.uireport(), default=date_handler),snapshot=util.getCurrentSnapshot())

        
class DataHandler(RequestHandler):
    @property
    def db(self):
        return self.application.db
    
    def get(self, method, snapshot):
        if method == 'overview':
            print self.db
            res = self.db.selectQuery("SELECT * FROM snapshot_stats WHERE snapshot=%s",tuple=(snapshot,))
            print res
            
            self.set_header('Content-Type', 'application/json')
            self.write(json_encode(res[0]))