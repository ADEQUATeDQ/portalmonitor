import copy
import glob
import json

from bokeh.embed import components
from bokeh.resources import INLINE
from flask import Flask, render_template, jsonify
from jinja2 import Environment, PackageLoader


from odpw.utils.timer import Timer
from odpw.testing.portal_report_plots import qualityChart, evolutionChart
from odpw.utils.util import tofirstdayinisoweek

env = Environment(loader=PackageLoader('odpw.testing.reports', 'templates'))

template = env.get_template('portal.jinja')

ex={}
ex['ExAc']={'label': 'Access', 'color':'#311B92' }
ex['ExCo']={'label': 'Contact', 'color':'#4527A0'}
ex['ExDa']={'label': 'Date', 'color':'#512DA8'}
ex['ExDi']={'label': 'Discovery', 'color':'#5E35B1'}
ex['ExPr']={'label': 'Preservation', 'color':'#673AB7'}
ex['ExRi']={'label': 'Rights', 'color':'#7E57C2'}
ex['ExSp']={'label': 'Spatial', 'color':'#9575CD'}
ex['ExTe']={'label': 'Temporal', 'color':'#B39DDB'}
existence={'dimension':'Existence','metrics':ex, 'color':'#B39DDB'}

ac={}
ac['AcFo']={'label': 'Format', 'color':'#00838F'}
ac['AcSi']={'label': 'Size', 'color':'#0097A7'}
accuracy={'dimension':'Accurracy', 'metrics':ac, 'color':'#0097A7'}

co={}
co['CoAc']={'label': 'AccessURL', 'color':'#388E3C'}
co['CoCE']={'label': 'ContactEmail', 'color':'#1B5E20'}
co['CoCU']={'label': 'ContactURL', 'color':'#43A047'}
co['CoDa']={'label': 'DateFormat', 'color':'#66BB6A'}
co['CoFo']={'label': 'FileFormat', 'color':'#A5D6A7'}
co['CoLi']={'label': 'License', 'color':'#C8E6C9'}
conformance={'dimension':'Conformance', 'metrics':co, 'color':'#C8E6C9'}

op={}
op['OpFo']={'label': 'Format Openness information', 'color':'#F4511E'}
op['OpLi']={'label': 'License Openneness', 'color':'#FF8A65'}
op['OpMa']={'label': 'Format machine readability', 'color':'#E64A19'}
opendata={'dimension':'Open Data', 'metrics':op, 'color':'#E64A19'}

re={}
re['ReDa']={'label': 'Datasets', 'color':'#FF9800'}
re['ReRe']={'label': 'Resources', 'color':'#FFA726'}
retrievability={'dimension':'Retrievability', 'metrics':re, 'color':'#FFA726'}

qa=[existence, conformance, opendata]#, retrievability, accuracy]

def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    """
    import unicodedata
    import re
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = unicode(re.sub('[^\w\s-]', '', value).strip().lower())
    value= re.sub('[-\s]+', '-', value)
    value=value.replace(" ","_")
    return value

import os
import pandas as pd
from dateutil.parser import parse
import collections
import time



app = Flask(__name__)
cache={}


def render(data, evolv, nav=None):
    print data.keys()
    print evolv
    print 'nav',nav

    with Timer(key="dataDF", verbose=True) as t:
        d=[]
        datasets= data['DatasetCount']['count']
        for inD in qa:
            for k,v in inD['metrics'].items():

                value=data['DCATDMD'][k]['value']
                perc=data['DCATDMD'][k]['total']/(datasets*1.0) if datasets>0 else 0
                c= { 'Metric':k, 'Dimension':inD['dimension'],
                     'dim_color':inD['color'], 'value':value, 'perc':perc}
                c.update(v)
                d.append(c)

        df1 = pd.DataFrame(d)

    with Timer(key="evolvDF", verbose=True) as t:
        edata=[]

        for k,v in collections.OrderedDict(sorted(evolv['snapshots'].items())).items():
            d={ 'alpha':0.5, 'hide':0}
            for vk,vv in v.items():
                if vk=='date' :
                    d[vk]= time.mktime(parse(vv).timetuple())* 1000
                elif vk =='datasets':
                    d[vk]=vv
                elif isinstance(vv, dict):
                    for vvk,vvv in vv.items():
                        if isinstance(vvv, dict):
                            for vvvk,vvvv in vvv.items():
                                d["".join([vk,vvk,vvvk])]=vvvv
                        else:
                            d["".join([vk,vvk])]=vvv
                else:
                    d[vk]=vv

            edata.append(d)
        evolvdf= pd.DataFrame(edata)
    with Timer(key="qualityChart", verbose=True) as t:
        p= qualityChart(df1)
        js, plot_dicts= components({ "graph": p})
        resources = INLINE
        js_resources = resources.render_js()
        css_resources = resources.render_css()

    with Timer(key="evolChart", verbose=True) as t:
        evol_plots = evolutionChart(evolvdf)

    with Timer(key="renderHTML", verbose=True) as t:
        return render_template('portal.jinja', data=data, evol=evolv, nav=nav, gennav=cache['genNav'], qa_labels=qa,plot_script=js, plot_div=plot_dicts, js_resources=js_resources,
                       css_resources=css_resources, evolplots=evol_plots)


def listdir_nohidden(path):
    #print "listdir", path
    for f in os.listdir(path):
        if not f.startswith('.'):
            if os.path.isdir(os.path.join(path,f)):
                yield f



def parseFromDisc(fname, software, iso):

    result={'software_nav':[x for x in listdir_nohidden(fname)]}
    fname= os.path.join(fname,software)


    result['iso_nav']=[x[0] for x in listdir_nohidden(fname)]

    print result

    return result

def navigation_fromDisc( base):
    results={}

    softF=os.path.join(*[base,'software'])
    for sn in listdir_nohidden(softF):
        if sn.startswith('1'):
            softInSN= os.path.join(softF,sn)
            for software in listdir_nohidden(softInSN):
                d=results.setdefault(software,{'since':sn,'since_date':str(tofirstdayinisoweek(sn).date()) , 'iso':{}})

                if d['since']>sn:
                    d['since']=sn
                    d['since_date']=tofirstdayinisoweek(sn).date()

                isoF= os.path.join(softInSN,software)
                data = json.load(open(isoF+'.json'))

                for iso, portals in data['portalIDsByISO'].items():
                    dd=d['iso'].setdefault(iso,{'since':sn,'since_date':str(tofirstdayinisoweek(sn).date()),'portals':{}})
                    if dd['since']>sn:
                        dd['since']=sn
                        d['since_date']=tofirstdayinisoweek(sn).date()
                    for p in portals:
                        if p not in dd['portals'] or dd['portals'][p]>sn:
                            dd['portals'][p]={'sn':sn,'date':str(tofirstdayinisoweek(sn).date())}


    print results
    return results





def navigation(snapshot, portal_id=None, software=None, iso=None):
    """
    Get the necessary navigation data
    :param snapshot:
    :param portal_id:
    :param software:
    :param iso:
    :return:
    """

    #check cache
    with Timer(key="navigation", verbose=True) as t:
        cache.setdefault(snapshot, {})
        if portal_id:
            if portal_id not in cache[snapshot]:
                fname=os.path.join(*[app.config['base'],'portal', str(snapshot),portal_id])
                data = json.load(open(fname+'.json'))

                cache[snapshot][portal_id]={'iso3':data['iso3'], 'software':data['software'], 'organisations':data['DCATOrganizationsCount'].keys()}
            return cache[snapshot][portal_id]

    # if not , get data




@app.route("/iso/<snapshot>/<iso>")
def iso(iso, snapshot):

    with Timer(key="software",verbose=True) as t:
        fname=os.path.join(*[app.config['base'],'iso', snapshot,iso])
        print fname

        data = json.load(open(fname+'.json'))
        fname=os.path.join(*[app.config['base'],'iso', 'evol',iso])
        evolv = json.load(open(fname+'_evolution.json'))
        return render(data, evolv)


@app.route("/software/<snapshot>/<soft>")
def software(soft, snapshot):

    with Timer(key="software",verbose=True) as t:
        fname=os.path.join(*[app.config['base'],'software', snapshot,soft])
        print fname

        data = json.load(open(fname+'.json'))
        fname=os.path.join(*[app.config['base'],'software', 'evol',soft])
        evolv = json.load(open(fname+'_evolution.json'))
        return render(data, evolv)

@app.route("/software/<snapshot>/<soft>/<iso>/")
def softwareISO(soft, iso,  snapshot):

    with Timer(key="softwareISO",verbose=True) as t:
        fname=os.path.join(*[app.config['base'],'software', snapshot, soft, iso])
        print fname

        data = json.load(open(fname+'.json'))
        fname=os.path.join(*[app.config['base'],'software', 'evol',soft,iso])
        evolv = json.load(open(fname+'_evolution.json'))
        return render(data, evolv)

@app.route("/portal/<snapshot>/<portal_id>")
@app.route("/portal/<snapshot>/<portal_id>/")
def portal(portal_id, snapshot):
    with Timer(key="portal", verbose=True) as t:
        with Timer(key="fileLoading", verbose=True) as t:
            fname=os.path.join(*[app.config['base'], 'portal', snapshot, portal_id])
            data = json.load(open(fname+'.json'))
            fname=os.path.join(*[app.config['base'],'portal', 'evol',portal_id])
            evolv = json.load(open(fname+'_evolution.json'))

        software= data['software'] if 'software' in data else None
        iso= data['iso'] if 'iso' in data else None
        nav = navigation(snapshot, software=software, iso=iso)

        return render(data, evolv, nav)

@app.route("/portal/<snapshot>/<portal_id>/<orga>/")
def orga(portal_id, orga, snapshot):
    with Timer(key="orga", verbose=True) as t:
        fname=os.path.join(*[app.config['base'],'portal', str(snapshot), portal_id, orga])

        nav = navigation(snapshot, portal_id=portal_id)

        data = json.load(open(fname+'.json'))
        fname=os.path.join(*[app.config['base'],'portal', 'evol', portal_id, orga])
        evolv = json.load(open(fname+'_evolution.json'))
        return render(data, evolv, nav)

@app.route("/")
def index():
    """Print available functions."""
    func_list = {}
    for rule in app.url_map.iter_rules():
        if rule.endpoint != 'static':
            func_list[rule.rule] = app.view_functions[rule.endpoint].__doc__
    return jsonify(func_list)

@app.route("/list")
def list():
    """Print available functions."""

    return jsonify(cache['genNav'])




def main():
    app.logger.info('Starting OPDW Dashboard on http://localhost:{}/'.format(1111))
    print 'Starting OPDW Dashboard on http://localhost:{}/'.format(1111)
    app.jinja_env.globals.update(slugify=slugify)

    app.config['base']=u'/Users/jumbrich/Data/portal_stats/'

    cache['genNav']=navigation_fromDisc(app.config['base'])
    app.run(debug=True, port = 1111)



if __name__ == "__main__":
    main()