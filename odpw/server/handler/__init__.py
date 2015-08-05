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
from odpw.analysers import process_all

import sys
import traceback
import os
from odpw.analysers.pmd_analysers import PMDActivityAnalyser


class BaseHandler(RequestHandler):
    @property
    def env(self):
        return self.application.env

    @property
    def db(self):
        return self.application.db
    @property
    def printHtml(self):
        return self.application.printHtml

    def get_error_html(self, status_code, **kwargs):
        print "error", status_code, kwargs
        try:
            self.render('error/{}.html'.format(status_code))
        except (TemplateNotFound, HTTPError):
            try:
                self.render('error/50x.html', status_code=status_code)
            except (TemplateNotFound, HTTPError):
                self.write("You aren't supposed to see this")

    def render(self, templateName, **kwargs):
        try:
            template = self.env.get_template(templateName)
        except TemplateNotFound:
            raise HTTPError(404)
        self.env.globals['static_url'] = self.static_url
        if self.printHtml:
            try:
                with open(os.path.join(os.path.dirname(self.printHtml) ,templateName+'.html'), "w") as f:
                    f.write(template.render(kwargs ))
            except TemplateNotFound as e:
                print e
        
        
        self.write(template.render(kwargs))


class NoDestinationHandler(RequestHandler):
    def get(self):
        raise HTTPError(503)

class PortalHandler(BaseHandler):
    def get(self, **params):
        print 'kwargs',params
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
            a = process_all(DBAnalyser(),self.db.getSoftwareDist())
            ab = process_all(DBAnalyser(),self.db.getCountryDist())
            sys_or = ReporterEngine([SoftWareDistReporter(a),
                 ISO3DistReporter(ab)])
            
            args={'index':True,
               'data':sys_or.uireport()
            }    
            self.render('index.html',**args)
        
class SystemActivityHandler(BaseHandler):
    def get(self):
        
        sn = util.getCurrentSnapshot()
        it =self.dbm.getPortalMetaDatas(snapshot=sn,portalID=None)
        a = process_all(PMDActivityAnalyser(),it)
        
        report = ReporterEngine([SystemActivityReporter(a)])
            
        args={'index':True,
               'data':report.uireport(),
               'snapshot':util.getCurrentSnapshot()
        }    
        self.render('system_activity.jinja',args)

        
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