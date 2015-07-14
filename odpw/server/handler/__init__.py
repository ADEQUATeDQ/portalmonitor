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


class PortalList(BaseHandler):
    def get(self):
        
        portals=[]
        for pRes in self.db.getPortals():
            portals.append(dict(pRes))
        
        self.render('portallist.jinja',index=True, data=portals)

class IndexHandler(BaseHandler):
    def get(self):
        
        software=defaultdict(int)
        countryDist=defaultdict(int)
        tldDist=defaultdict(int)
        
        res=0
        ds=0
        
        for pRes in self.db.getPortals():
            p = Portal.fromResult(dict(pRes))
        
        
            if p.datasets !=-1:
                ds+=p.datasets
            if p.resources != -1:
                res+=p.resources

            url_elements = urlparse(p.url).netloc.split(".")
            tld = ".".join(url_elements[-1:])
            tldDist[tld]+=1
            countryDist[p.country]+=1
            software[p.__dict__['software']]+=1
        
        data={
              'tlddist':dict(tldDist),
              'softwaredist':[{'key':k,'value':v} for k, v in software.iteritems()],
              'countrydist':dict(countryDist),
              'resources':res,
              'datasets':ds
              }
        self.render('index.html',index=True, data=json.dumps(data))
        
        
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