#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tornado.web import RequestHandler, HTTPError
from jinja2.exceptions import TemplateNotFound
from tornado.escape import json_encode

from odpw.db.models import Portal, PortalMetaData
from odpw.utils  import util
from collections import defaultdict
from urlparse import urlparse 
import json
from odpw.db.dbm import nested_json, date_handler
from odpw.reporting.reporters import DBAnalyser, DFtoListDict, addPercentageCol,\
    Report, ISO3DistReporter, SoftWareDistReporter,\
    SystemActivityReporter, SnapshotsPerPortalReporter, LicensesReporter,\
    TagReporter, OrganisationReporter, FormatCountReporter, DatasetSumReporter,\
    ResourceSumReporter
from odpw.utils.timer import Timer
from odpw.analysers import process_all, AnalyserSet

import sys
import traceback
import os
from odpw.analysers.pmd_analysers import PMDActivityAnalyser
from odpw.analysers.fetching import CKANLicenseCount, CKANLicenseConformance,\
    CKANTagsCount, CKANOrganizationsCount, CKANFormatCount,\
    DatasetCount
from odpw.analysers.resource_analysers import ResourceCount


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
        
    def _handle_request_exception(self, e):
        print e
        traceback.print_exc(file=sys.stdout)


class NoDestinationHandler(RequestHandler):
    def get(self):
        raise HTTPError(503)

class PortalSelectionHandler(BaseHandler):
    def get(self, **params):
        a= process_all( DBAnalyser(), self.db.getSnapshots( portalID=None,apiurl=None))
        rep = Report([SnapshotsPerPortalReporter(a,None)])
            
        self.render('portal_empty.jinja',portals=True, data=rep.uireport())
    
class PortalHandler(BaseHandler):
    def get(self, view=None, portalID=None, snapshot=None):
        
        a= process_all( DBAnalyser(), self.db.getSnapshots( portalID=None,apiurl=None))
        rep = SnapshotsPerPortalReporter(a,None)
        if not portalID:
            self.render('portal_empty.jinja', portals=True, data=rep.uireport())
    
        
        if portalID:
            if len(portalID.split(",")) ==1:
                if view == 'info':
                    aset = AnalyserSet()
                    lc=aset.add(CKANLicenseCount())# how many licenses
                    lcc=aset.add(CKANLicenseConformance())
        
                    tc= aset.add(CKANTagsCount())   # how many tags
                    oc= aset.add(CKANOrganizationsCount())# how many organisations
                    fc= aset.add(CKANFormatCount())# how many formats
    
                    resC= aset.add(ResourceCount())   # how many resources
                    dsC=dc= aset.add(DatasetCount())    # how many datasets
    
                    #use the latest portal meta data object
                    if not snapshot:
                        pmd = self.db.getLatestPortalMetaData(portalID=portalID)
                    else:
                        pmd = self.db.getPortalMetaData(portalID=portalID, snapshot=snapshot)
                    aset = process_all(aset, [pmd])
        
                    rep = Report([rep,
                    DatasetSumReporter(resC),
                    ResourceSumReporter(dsC),
                    LicensesReporter(lc,lcc,topK=3),
                    TagReporter(tc,dc, topK=3),
                    OrganisationReporter(oc, topK=3),
                    FormatCountReporter(fc, topK=3)])
    
                    self.render('portal_info.jinja', portals=True,data=rep.uireport(), portalID=portalID, snapshot=snapshot)
            else:
                self.render('portals_detail.jinja', portals=True)
            
                
        
        
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
        
                self.render('portallist.jinja',index=True, data=portals)
            except Exception as e:print e

class IndexHandler(BaseHandler):
    def get(self):
            a = process_all(DBAnalyser(),self.db.getSoftwareDist())
            ab = process_all(DBAnalyser(),self.db.getCountryDist())
            r = Report([SoftWareDistReporter(a),
                 ISO3DistReporter(ab)])
            args={
                  'index':True,
                  'data':r.uireport()
            }    
            self.render('index.jinja',**args)
        
class SystemActivityHandler(BaseHandler):
    def get(self, snapshot=None):
        try:
            if not snapshot:
                sn = util.getCurrentSnapshot()
            else:
                sn=snapshot
        
            it =PortalMetaData.iter(self.db.getPortalMetaDatas(snapshot=sn, portalID=None))
            a = process_all(PMDActivityAnalyser(),it)
        
        
            report = Report([SystemActivityReporter(a,snapshot=sn)])
        
            args={'index':True,
               'data':report.uireport(),
               'snapshot':sn
            }    
            self.render('system_activity.jinja',**args)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

        
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