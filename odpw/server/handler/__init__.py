#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tornado.web import RequestHandler, HTTPError
from jinja2.exceptions import TemplateNotFound
from tornado.escape import json_encode

from odpw.db.models import Portal
from odpw  import util
from collections import defaultdict
from urlparse import urlparse 
import json
from odpw.db.dbm import nested_json, date_handler
from odpw.reporting.reporters import DBAnalyser, DFtoListDict, addPercentageCol


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
            self.render('portal_detail_empty.jinja',portals=True)
        elif params['portal']:
            if len(params['portal'].split(",")) ==1:
                pid=params['portal']
                print 'get '
                #r = PortalOverviewReporter(self.db, portalID=pid)
                
                
                self.render('portal_detail.jinja',portals=True)
            else:
                self.render('portals_detail.jinja',portals=True)
            
                
        
        
class PortalList(BaseHandler):
    def get(self):
        
        portals=[]
        for pRes in self.db.getPortals():
            portals.append(dict(pRes))
        
        self.render('portallist.jinja',index=True, data=portals, json=json.dumps(portals, default=date_handler))

class IndexHandler(BaseHandler):
    def get(self):
        
        d = DBAnalyser(self.db.getSoftwareDist)
        d.analyse()
        softdist=DFtoListDict(d.getDataFrame())
        
        print softdist
        
        d = DBAnalyser(self.db.getCountryDist)
        d.analyse()
        countrydist=DFtoListDict(addPercentageCol(d.getDataFrame()))
        
        
        d = DBAnalyser(self.db.getPMDStatusDist)
        d.analyse()
        pmdStatus=DFtoListDict((d.getDataFrame()))
        
        data={'softdist':softdist,'countrydist':countrydist,'pmdStatus':pmdStatus}
            
        self.render('index.html',index=True, data=data)
        
        
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