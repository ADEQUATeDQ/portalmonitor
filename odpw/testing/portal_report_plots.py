import json
from datetime import date

from bokeh.charts import TimeSeries, Bar
from bokeh.io import vplot,hplot
from bokeh.models import ColumnDataSource, HoverTool, FixedTicker, NumeralTickFormatter, Select, VBoxForm, HBox, \
    CustomJS, Line, DatetimeTickFormatter

from odpw.utils.timer import Timer


ot={}
ot['datasets']={'label':'Datasets','color':''}
ot['formats']={'label':'Formats','color':''}
ot['licenses']={'label':'Licenses','color':''}
ot['organisations']={'label':'organisations','color':''}
ot['tags']={'label':'tags','color':''}
others={'dimension':'Others','metrics':ot, 'color':'#B39DDB'}



dis={}
dis['distributionscount']={'label':'total','color':''}
dis['distributionsdistinct']={'label':'distinct','color':''}
dis['distributionsurls']={'label':'urls','color':''}
dis['distributionsempty']={'label':'empty','color':''}
dis['distributionshttps_urls']={'label':'https','color':''}

distribution={'dimension':'Distribution','metrics':dis, 'color':'#B39DDB'}

ex={}
ex['qualityExAcvalue']={'label': 'Access', 'color':'#311B92' }
ex['qualityExCovalue']={'label': 'Contact', 'color':'#4527A0'}
ex['qualityExDavalue']={'label': 'Date', 'color':'#512DA8'}
ex['qualityExDivalue']={'label': 'Discovery', 'color':'#5E35B1'}
ex['qualityExPrvalue']={'label': 'Preservation', 'color':'#673AB7'}
ex['qualityExRivalue']={'label': 'Rights', 'color':'#7E57C2'}
ex['qualityExSpvalue']={'label': 'Spatial', 'color':'#9575CD'}
ex['qualityExTevalue']={'label': 'Temporal', 'color':'#B39DDB'}
existence={'dimension':'Existence','metrics':ex, 'color':'#B39DDB'}

ac={}
ac['AcFo']={'label': 'Format', 'color':'#00838F'}
ac['AcSi']={'label': 'Size', 'color':'#0097A7'}
accuracy={'dimension':'Accurracy', 'metrics':ac, 'color':'#0097A7'}

co={}
co['qualityCoAcvalue']={'label': 'AccessURL', 'color':'#388E3C'}
co['qualityCoCEvalue']={'label': 'ContactEmail', 'color':'#1B5E20'}
co['qualityCoCUvalue']={'label': 'ContactURL', 'color':'#43A047'}
co['qualityCoDavalue']={'label': 'DateFormat', 'color':'#66BB6A'}
co['qualityCoFovalue']={'label': 'FileFormat', 'color':'#A5D6A7'}
co['qualityCoLivalue']={'label': 'License', 'color':'#C8E6C9'}
conformance={'dimension':'Conformance', 'metrics':co, 'color':'#C8E6C9'}

op={}
op['qualityOpFovalue']={'label': 'Format Openness information', 'color':'#F4511E'}
op['qualityOpLivalue']={'label': 'License Openneness', 'color':'#FF8A65'}
op['qualityOpMavalue']={'label': 'Format machine readability', 'color':'#E64A19'}
opendata={'dimension':'Open Data', 'metrics':op, 'color':'#E64A19'}

re={}
re['ReDa']={'label': 'Datasets', 'color':'#FF9800'}
re['ReRe']={'label': 'Resources', 'color':'#FFA726'}
retrievability={'dimension':'Retrievability', 'metrics':re, 'color':'#FFA726'}

qa=[existence, conformance, opendata, others,distribution]#, retrievability, accuracy]

from bokeh.plotting import figure, show, output_file
TOOLS="resize,crosshair,pan,wheel_zoom,box_zoom,undo,redo,reset,tap,previewsave,box_select,poly_select,lasso_select"





import numpy as np

def qualityChart(df):

    dim_color = {}
    key_color = {}
    for index, r  in df.iterrows():
        dim_color[r['Dimension']] =r['dim_color']
        key_color[r['Metric']]= r['color']

    width = 800
    height = 800

    inner_radius = 90
    outer_radius = 300 - 10

    minr = 0 #sqrt(log(0 * 1E4))
    maxr = 1#sqrt(log(1 * 1E4))
    a = (outer_radius - inner_radius) / (maxr - minr)
    b = inner_radius

    def rad(mic):
        v = a * mic + b
        return v

    big_angle = 2.0 * np.pi / (len(df) + 1)
    small_angle = big_angle / 7

    x = np.zeros(len(df))
    y = np.zeros(len(df))


    tools = "reset"
    # create chart
    p = figure(plot_width=width, plot_height=height, title="",
        x_axis_type=None, y_axis_type=None,
        x_range=[-420, 420], y_range=[-420, 420],
        min_border=0
        #,tools=tools
        #outline_line_color="black",
        #background_fill="#f0e1d2",
        #border_fill="#f0e1d2"
        )

    p.line(x+1, y+1, alpha=0.5)

    # DIMENSION CIRCLE
    angles = np.pi/2 - big_angle/2 - df.index.to_series()*big_angle
    colors = [dim_color[dim] for dim in df.Dimension]
    p.annular_wedge(
        x, y, outer_radius+15, outer_radius+30, -big_angle+angles, angles, color=colors,
    )

    source = ColumnDataSource(df)
    kcolors = [key_color[k] for k in df.Metric]
    g_r1= p.annular_wedge(x, y, inner_radius, rad(df.value),
        -big_angle+ angles+3*small_angle, -big_angle+angles+6*small_angle,
        color=kcolors, source=source)

    p.annular_wedge(x, y, inner_radius, rad(df.perc),
        -big_angle+ angles+2.5*small_angle, -big_angle+angles+6.5*small_angle, alpha=0.4,
        color='grey')



    g1_hover = HoverTool(renderers=[g_r1],
                         tooltips=[('value', '@value'), ('Metric', '@label'),('Dimension', '@Dimension')])
    p.add_tools(g1_hover)
    #Mrtrics labels
    labels = np.array([c / 100.0 for c in range(0, 110, 10)]) #
    radii = a * labels + b

    p.circle(x, y, radius=radii, fill_color=None, line_color="#d3d3d3")
    p.annular_wedge([0], [0], inner_radius-10, outer_radius+10,
        0.48*np.pi, 0.52 * np.pi, color="white")

    p.text(x, radii, [str(r) for r in labels],
        text_font_size="8pt", text_align="center", text_baseline="middle")

    # radial axes
    p.annular_wedge(x, y, inner_radius, outer_radius+10,
        -big_angle+angles, -big_angle+angles, color="black")


    # Dimension labels
    xr = radii[5]*np.cos(np.array(-big_angle/1.25 + angles))
    yr = radii[5]*np.sin(np.array(-big_angle/1.25 + angles))

    label_angle=np.array(-big_angle/1.4+angles)
    label_angle[label_angle < -np.pi/2] += np.pi # easier to read labels on the left side
    p.text(xr, yr, df.label, angle=label_angle,
        text_font_size="9pt", text_align="center", text_baseline="middle")


    #dim legend
    p.rect([-40,-40, -40, -40,-40], [36,18, 0, -18, -36], width=30, height=13,
        color=list(dim_color.values()))
    p.text([-15,-15, -15, -15,-15], [36,18, 0, -18,-36], text=list(dim_color.keys()),
        text_font_size="9pt", text_align="left", text_baseline="middle")

    #p.logo = None
    #p.toolbar_location = None

    return p
    #return components({ "graph": p}, wrap_script=False,wrap_plot_info=False)



def qa_bar_plots(j_data):
    print j_data['total']['DCATDMD']
    from bokeh.sampledata.autompg import autompg as df
    print df
    plots=[]
    for k,v in j_data['total']['DCATDMD'].items():
        data=[]
        for m,c in v['hist'].items():
            data.append({'m':m,'value':c})
        bar = Bar(data, 'm', values='value',title=k, plot_width=400)
        plots.append(bar)

    return plots

def ds_evolv(source):
    p_ds = figure(width=800, height=250,x_axis_type="datetime",title=None, toolbar_location=None)

    #datasets
    print source.data['date']
    p_ds.line(x='date', y='datasets', color='navy', alpha=0.5,source=source)
    c=p_ds.circle( x='date', y='datasets', color='navy', alpha=0.5,fill_color="white", size=8, source=source, name="reds")
    p_ds.xaxis.axis_label='snapshot'
    p_ds.yaxis.axis_label='datasets'
    p_ds.xaxis.formatter=DatetimeTickFormatter(formats=dict(
        hours=["%Y-%m-%d"],
        days=["%Y-%m-%d"],
        months=["%Y-%m-%d"],
        years=["%Y-%m-%d"],
    ))

    return p_ds







    #,u'qualityExRitotal', , u'qualityExSptotal',
    #   , u'qualityExTetotal', ,
    #   u'qualityOpFototal', , u'qualityOpLitotal',
    #   , u'qualityOpMatotal',
    #   u'snapshot',

#(u'qualityExActotal'),
#(u'qualityExCototal')
#(u'qualityExDatotal')
#(u'qualityExDitotal')
#(u'qualityExPrtotal')


def res_evolv(source):
    ph = figure(toolbar_location=None, plot_width=800, plot_height=200,
             title=None, min_border=10, min_border_left=50,x_axis_type="datetime")

    for l,v in distribution.items():
        ph.line(x='date', y=l[0], color=l[1], alpha=0.5,source=source, legend=l[2])
        ph.circle( x='date', y=l[0], color=l[1], alpha=0.5,fill_color="white", size=8, source=source, name="res")

    ph.yaxis.axis_label='resources'
    ph.xgrid.grid_line_color = None
    ph.xaxis.visible = None

    ph.min_border_top = 10
    ph.min_border_right = 10
    return ph


def qa_evolv(source):
    qa_plots=[]
    for k in qa:
        p_qa = figure(toolbar_location=None, plot_width=800, plot_height=200,
                 title=None, min_border=10, min_border_left=50,x_axis_type="datetime")
        for m,v in k['metrics'].items():
            p_qa.line(x='time', y=m, color=v['color'], alpha=0.5,source=source)
            p_qa.circle( x='time', y=m, color=v['color'], alpha=0.5,fill_color="white", size=4, source=source, name="reds")
            p_qa.yaxis.axis_label='QScore'
        qa_plots.append(p_qa)

    return qa_plots

def evolvPlots(source):
    plots=[]
    with Timer(verbose=True) as t:


        plots.append(ds_evolv(source))
        plots.append(res_evolv(source))
        #plots=plots+(qa_evolv(source))
        #plots.append(qualityChart(df1))
        #plots=plots+ qa_bar_plots(j1)
        #resources

    return plots

def evolutionChart(df):
    from nvd3 import lineChart

    plots={}

    for d in qa:
        dl=d['dimension'].replace(" ","_")


        chart = lineChart(name=dl, x_is_date=True, date_format="%d %b %Y",color_category='category20c',
                          display_container=False, resize=True, jquery_on_ready=False,height=200, width=800, style="width:100%;")

        for l,v in d['metrics'].items():
            es = {"tooltip": {"y_start": "There are ", "y_end": " calls"}}
            chart.add_serie(y=df[l], x=df['date'], name=v['label'], extra=es)



        chart.buildhtml()
        chart.buildjschart()
        chart.buildcontent()

        c=chart.htmlcontent
        c=c.replace("<script>","")
        c=c.replace("</script>","")

        o_file = open(dl+'.html', 'w')
        o_file.write(chart.htmlcontent)
        o_file.close()

        plots[dl]={'div':chart.container,'js':c}

    return plots


if __name__ == '__main__':

    portal="opendata_awt_be"

    j1 = json.load(open('/Users/jumbrich/Dev/odpw/stats/portal/1503/'+portal+'.json'))
    evolv = json.load(open('/Users/jumbrich/Dev/odpw/stats/portal/evol/'+portal+'_evolution.json'))
    from pprint import pprint
    import time


    data=[]
    from dateutil.parser import parse
    import collections

    res={}
    l=[]
    for k,v in collections.OrderedDict(sorted(evolv['snapshots'].items())).items():
        d={ 'alpha':0.5, 'hide':0}
        for vk,vv in v.items():
            print vk
            if vk=='date' :
                print '  ',vk, vv
                d[vk]= time.mktime(parse(vv).timetuple())* 1000
            elif vk =='datasets':
                d[vk]=vv
            elif isinstance(vv, dict):

                if vk=='distributions':
                    for vvk,vvv in vv.items():
                        res.setdefault(vvk,[])
                        res[vvk].append({'date': parse(v['date']),
                                         'value':vvv,'hide':0})



                for vvk,vvv in vv.items():

                    if isinstance(vvv, dict):
                        for vvvk,vvvv in vvv.items():
                            d["".join([vk,vvk,vvvk])]=vvvv
                    else:
                        d["".join([vk,vvk])]=vvv
            else:
                d[vk]=vv

        data.append(d)
        l.append(str(k))

    print res
    datasets= j1['DatasetCount']['count']
    d=[]
    for inD in qa:
        for k,v in inD['metrics'].items():
            if k in j1['DCATDMD']:
                value=j1['DCATDMD'][k]['value']
                perc=j1['DCATDMD'][k]['total']/(datasets*1.0)
                c= { 'Metric':k, 'Dimension':inD['dimension'],
                 'dim_color':inD['color'], 'value':value, 'perc':perc}
                c.update(v)
                d.append(c)

    import pandas as pd
    df1 = pd.DataFrame(d)
    evolvdf= pd.DataFrame(data)


    #pprint(evolvdf)
    pprint(evolvdf.columns.values)


    from nvd3 import lineChart


    plots={}

    for d in qa:
        dl=d['dimension'].replace(" ","_")


        chart = lineChart(name=dl, x_is_date=True, date_format="%d %b %Y",color_category='category20c',
                          display_container=False,resize=True,jquery_on_ready=True)

        xdata = evolvdf['date']


        for l,v in d['metrics'].items():
            extra_serie = {"tooltip": {"y_start": "There are ", "y_end": " calls"}}
            chart.add_serie(y=evolvdf[l], x=evolvdf['date'], name=v['label'], extra=extra_serie)

        chart.set_graph_width(800)
        chart.buildhtml()
        chart.buildjschart()
        chart.buildcontent()


        plots[dl]={'div':chart.container,'data':chart.series_js,'js':chart.htmlcontent}

        o_file = open(dl+'.html', 'w')
        o_file.write(chart.htmlcontent)
        o_file.close()

    pprint(plots)
    source = ColumnDataSource(data=evolvdf)

    output_file("color_scatter.html", title="")
    #plots = evolvPlots(source)
    #print plots
    #show(vplot(*plots))

    source_res={ k: ColumnDataSource(data=pd.DataFrame(v)) for k,v in res.items()}

    del source_res['https_urls']
    source_res['none']=ColumnDataSource(data=pd.DataFrame([]))
    print source_res


    source_res={'one':ColumnDataSource(data=pd.DataFrame([{'x':1,'y':10,'v':10,'n':0,'s':1},{'x':2,'y':20,'v':20, 'n':0,'s':10,'alpha':1}]))
                ,'two':ColumnDataSource(data=pd.DataFrame([{'x':1,'y':5},{'x':2,'y':15}])),
                }

    source=ColumnDataSource(data=pd.DataFrame([{'x':1,'y':10,'y2':5},{'x':2,'y':20,'y2':0}]))
    pmetric = figure(toolbar_location=None, plot_width=500, plot_height=200,
              title=None, min_border=10, min_border_left=50)

    for k,s in source_res.items():
        l=pmetric.line(x='x', y='y', color='navy', line_width='alpha', alpha='alpha',source=s)
        c=pmetric.circle( x='x', y='y', color='navy', alpha='alpha',fill_color="white", size='s', source=s, name=k)
    callback = CustomJS(args=source_res, code="""
         var f = cb_obj.get('value');

         console.log(eval(f))
         if( eval(f) != one ){

            one.get("data")['s']=one.get("data")['n']
            one.get("data")['alpha']=one.get("data")['n']
            one.trigger('change');
         }




     """)


    metric = Select(title="Metric", options=sorted(list(source_res.keys())), value="OpLi",callback=callback)

    show(vplot(HBox(VBoxForm(metric), width=300),pmetric))


    import sys
    sys.exit(0)

#vplot(HBox(VBoxForm(metric), width=300),pmetric)))
#evolvPlots(evolvdf)
#import sys
#sys.exit(0)

# mdf=df.copy()
# mdf['y']=df['datasets']
# mdf['yl']=df['datasets']
# msource = ColumnDataSource(data=mdf)
# pmetric = figure(toolbar_location=None, plot_width=p.plot_width, plot_height=200,
#              title=None, min_border=10, min_border_left=50,x_axis_type="datetime")
# l=Line(x='time', y='yl', line_color='red', line_alpha=0.5, line_width=1)
# #l=pmetric.line(x='time', y='y', color='alpha', line_width='alpha', alpha='alpha',source=msource)
# c=pmetric.circle( x='time', y='y', color='navy', alpha='alpha',fill_color="white", size=8, source=msource, name="reds")
# pmetric.add_glyph(msource,l)
#
#
#
# def update(attr, old, new):
#     print update
#     msource.data['y']=df[metric.value]
#
# callback = CustomJS(args=dict(source=msource), code="""
#         var f = cb_obj.get('value');
#         console.log(f)
#         var r = source.get('data')[f];
#         source.get('data')["y"]=r;
#         source.get('data')["yl"]=source.get('data')['hide'];
#         alert(source.get('data'));
#         source.trigger('change');
#     """)
# callback1 = CustomJS(args=dict(source=msource), code="""
#         var f = cb_obj.get('value');
#         console.log(f)
#         var r = source.get('data')[f];
#         source.get('data')["alpha"]=source.get('data')['hide'];
#         alert(source.get('data'));
#         console.log(source.get('data'));
#         source.trigger('change');
#     """)
#
#
#
#
# metric = Select(title="Metric", options=sorted(list(mdf.columns.values)), value="OpLi",callback=callback)
# #metric.on_change('value', update)
#
#
#
#
# update(None,None,None)
#
#
# #show(vplot(ph,p, vplot(HBox(VBoxForm(metric), width=300),pmetric)))