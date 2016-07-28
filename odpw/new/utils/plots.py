from collections import defaultdict

import numpy as np
from bokeh.layouts import column, row
from bokeh.models import NumeralTickFormatter, FuncTickFormatter, Text, Range1d, HoverTool, \
    ColumnDataSource, pd, Spacer, Circle, Legend, TextAnnotation, LinearAxis
from bokeh.plotting import figure
from numpy.ma import arange

from odpw.new.core.model import PortalSnapshotQuality, PortalSnapshot
from odpw.new.utils.utils_snapshot import getLastNSnapshots, getWeekString, getWeekString1
from odpw.new.web.rest.odpw_restapi_blueprint import row2dict


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
op['OpFo']={'label': 'Format Openness', 'color':'#F4511E'}
op['OpLi']={'label': 'License Openneness', 'color':'#FF8A65'}
op['OpMa']={'label': 'Format machine readability', 'color':'#E64A19'}
opendata={'dimension':'Open Data', 'metrics':op, 'color':'#E64A19'}

re={}
re['ReDa']={'label': 'Datasets', 'color':'#FF9800'}
re['ReRe']={'label': 'Resources', 'color':'#FFA726'}
retrievability={'dimension':'Retrievability', 'metrics':re, 'color':'#FFA726'}

qa=[existence, conformance, opendata]#, retrievability, accuracy]



def hm(sec):
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)

    if d==0:
        return "%sh %sm"% (h, m)
    else:
        return "%sd %sh %sm"% (d,h, m)







def fetchProcessChart(db, snapshot, n=10):

    snapshots=getLastNSnapshots(snapshot,n)
    nWeeksago=snapshots[-1]

    cnts=defaultdict(int)
    data={}
    for r in db.Session.query(PortalSnapshot.snapshot, PortalSnapshot.start, PortalSnapshot.end-PortalSnapshot.start).filter(PortalSnapshot.snapshot>nWeeksago):
        sn,start, dur = r[0], r[1],r[2]
        cnts[sn]+=1

        d=data.setdefault(sn,{})
        if dur is not None:
            ds=d.setdefault(start,[])
            ds.append(dur.total_seconds())

    for sn, d in data.items():
        dd=[]
        gstart= min(d.keys())

        for start, durations in d.items():
            for dur in durations:
                delta=( start-gstart).total_seconds() + dur
                dd.append(delta)
        data[sn]=dd


    print data

    from bokeh.palettes import OrRd9

    bp = figure(plot_width=600, plot_height=300,y_axis_type="datetime",responsive=True,tools='')#,toolbar_location=None
    bp.toolbar.logo = None
    bp.toolbar_location = None


    bp.xaxis[0].formatter = NumeralTickFormatter(format="0.0%")


    bp.yaxis.formatter=FuncTickFormatter.from_py_func(hm)

    bp.xaxis[0].axis_label = '% of portals'
    bp.yaxis[0].axis_label = 'Time elapsed'

    mx=None
    c=0


    for sn in sorted(data.keys()):
        d=data[sn]

        d_sorted = np.sort(np.array(d))
        y=[e for e in d_sorted] #datetime.datetime.fromtimestamp(e)
        x = 1. * arange(len(d)) / (len(d) - 1)
        mx=max(x) if max(x)>mx else mx
        #print x
        # plot the sorted data:



        if sn == 1630:
            ci=bp.circle(x,y, size=5, alpha=0.5,  color='red', legend="current week: "+getWeekString(sn))
            li=bp.line(x,y, line_width=2,line_color='red', legend="current week: "+getWeekString(sn))
        else:
            ci=bp.circle(x,y, size=5, alpha=0.5,  color='gray')
            li=bp.line(x,y, line_width=2,line_color='gray')
            #hit_target =Circle(x,y, size=10,line_color=None, fill_color=None)


            #c.select(dict(type=HoverTool)).tooltips = {"Week": "@week",m:"@"+m.lower()}
            #hit_renderers.append(hit_renderer)

            bp.add_tools(HoverTool(renderers=[li], tooltips={"Week": getWeekString(sn)}))

        c+=1
        #bp.text(,y[-1], line_width=2,line_color=OrRd9[c],legend=str(sn))

        no_olympics_glyph = Text(x=x[-1], y=y[-1], x_offset=100, text=["%s of %s portals"%(len(d), cnts[sn])],
            text_align="right", text_baseline="top",
            text_font_size="9pt", text_font_style="italic", text_color="black")
        bp.add_glyph(no_olympics_glyph)

    bp.set(x_range=Range1d(0, mx*1.2))
    bp.background_fill_color = "#fafafa"

    bp.legend.location = "top_left"

    return bp


def portalsScatter(df):
    df=df.fillna(0)

    def get_dataset(df, name):
        df1 = df[df['software'] == name].copy()
        print name,df1.describe()
        del df1['software']
        return df1

    ckan = get_dataset(df,"CKAN")
    socrata = get_dataset(df,"Socrata")
    opendatasoft = get_dataset(df,"OpenDataSoft")
    all=df

    hmax = 0
    vmax = 0
    print hmax, vmax

    p = figure(   plot_width=400, plot_height=400
                , min_border=10, min_border_left=50
                , toolbar_location="above",responsive=True
                ,y_axis_type="log",x_axis_type="log")
    p.toolbar.logo = None
    p.toolbar_location = None


    #p.xaxis[0].axis_label = '#Datasets'
    #p.yaxis[0].axis_label = '#Resources'
    p.background_fill_color = "#fafafa"


    ph = figure(toolbar_location=None, plot_width=p.plot_width, plot_height=200, x_range=p.x_range,
                 min_border=10, min_border_left=50, y_axis_location="right",x_axis_type="log",responsive=True)

    pv = figure(toolbar_location=None, plot_width=200, plot_height=p.plot_height,
                y_range=p.y_range, min_border=10, y_axis_location="right",y_axis_type="log",responsive=True)


    for i, item in enumerate([
                        #(all, 'All','black')
                        #(ckan,'CKAN', '#3A5785')
                        #,(socrata,'Socrata', 'green')
                        (opendatasoft,'OpenDataSoft', 'red')
                        ]):

        s,l,c=item
        source=ColumnDataSource(data=s)
        p.scatter(x='datasetCount', y='resourceCount', size=3, source=source, color=c, legend=l)


        # create the horizontal histogram
        maxV= s['datasetCount'].max()
        bins= 10 ** np.linspace(np.log10(1), np.log10(maxV), 10)
        hhist, hedges = np.histogram(s['datasetCount'], bins=bins)#[0,5,10,50,100,500,1000,5000,10000,50000,100000]
        hzeros = np.zeros(len(hedges)-1)
        hmax = max(hhist)*1.5 if max(hhist)*1.5>hmax else hmax

        LINE_ARGS = dict(color=c, line_color=None)


        ph.xgrid.grid_line_color = None
        #ph.yaxis.major_label_orientation = np.pi/4
        ph.background_fill_color = "#fafafa"

        ph.quad(bottom=0, left=hedges[:-1], right=hedges[1:], top=hhist, color=c, line_color=c, alpha=0.5)
        hh1 = ph.quad(bottom=0, left=hedges[:-1], right=hedges[1:], top=hzeros, alpha=0.5, **LINE_ARGS)
        hh2 = ph.quad(bottom=0, left=hedges[:-1], right=hedges[1:], top=hzeros, alpha=0.1, **LINE_ARGS)

        # create the vertical histogram
        maxV= s['resourceCount'].max()
        bins= 10 ** np.linspace(np.log10(1), np.log10(maxV), 10)
        vhist, vedges = np.histogram(s['resourceCount'], bins=bins)#[0,5,10,50,100,500,1000,5000,10000,50000,100000]
        vzeros = np.zeros(len(vedges)-1)
        vmax = max(vhist)*1.5 if max(vhist)*1.5>vmax else vmax


        pv.ygrid.grid_line_color = None
        #pv.xaxis.major_label_orientation = np.pi/4
        pv.background_fill_color = "#fafafa"

        pv.quad(left=0, bottom=vedges[:-1], top=vedges[1:], right=vhist, color=c, line_color=c, alpha=0.5)
        vh1 = pv.quad(left=0, bottom=vedges[:-1], top=vedges[1:], right=vzeros, alpha=0.5, **LINE_ARGS)
        vh2 = pv.quad(left=0, bottom=vedges[:-1], top=vedges[1:], right=vzeros, alpha=0.1, **LINE_ARGS)

    ph.set(y_range=Range1d(0.1, hmax))
    pv.set(x_range=Range1d(0.1, vmax))

    #plots={'scatter':p,'data':ph,'res':pv}
    p.legend.location = "bottom_right"

    layout = column(row(p, pv), row(ph, Spacer(width=200, height=200)))

    return layout

def qualityChart(df):

    print df

    dim_color = {}
    key_color = {}
    for index, r  in df.iterrows():
        dim_color[r['Dimension']] =r['dim_color']
        key_color[r['Metric']]= r['color']


    width = 400
    height = 400
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
    p = figure( plot_width=width, plot_height=height, title="",
        x_axis_type=None, y_axis_type=None,
        x_range=[-420, 420], y_range=[-420, 420],
        min_border=0
        ,responsive=True,tools=''
        #,tools=tools
        #outline_line_color="black",
        #background_fill="#f0e1d2",
        #border_fill="#f0e1d2"
        )
    p.toolbar.logo = None
    p.toolbar_location = None

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
                         tooltips=[('value', '@value'), ('Metric', '@label'),('Dimension', '@Dimension'),('Percentage', '@perc')])
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
    p.background_fill_color = "#fafafa"
    return p


def evolSize(source,df):
    p = figure(   plot_width=600, plot_height=200
                , min_border=10, min_border_left=50
                , toolbar_location="above",responsive=True)
    p.background_fill_color = "#fafafa"
    p.legend.location = "top_left"
    p.toolbar.logo = None
    p.toolbar_location = None

    legends=[]

    l=p.line(x='snapshot',y='datasetCount', line_width=2,source=source)
    c=p.circle(x='snapshot',y='datasetCount', line_width=2,source=source)

    hit_target =Circle(x='snapshot',y='datasetCount', size=10,line_color=None, fill_color=None)
    hit_renderer = p.add_glyph(source, hit_target)

    legends.append(("Datasets",[l,c]))
    p.add_tools(HoverTool(renderers=[hit_renderer], tooltips={'Metric':"Size", "Week": "@week", 'Value':"@datasetCount"}))

    #######
    l=p.line(x='snapshot',y='resourceCount', line_width=2,source=source)
    c=p.circle(x='snapshot',y='resourceCount', line_width=2,source=source)

    hit_target =Circle(x='snapshot',y='resourceCount', size=10,line_color=None, fill_color=None)
    hit_renderer = p.add_glyph(source, hit_target)

    legends.append(("Resources",[l,c]))
    p.add_tools(HoverTool(renderers=[hit_renderer], tooltips={'Metric':"Size", "Week": "@week", 'Value':"@resourceCount"}))


    p.xaxis[0].ticker.desired_num_ticks = df.shape[0]

    p.xaxis.formatter=FuncTickFormatter.from_py_func(getWeekString1)
    p.axis.minor_tick_line_color = None

    legend = Legend(legends=legends, location=(0, -30))

    p.add_layout(legend, 'right')

    p.xaxis[0].axis_label = 'Snapshot'
    p.yaxis[0].axis_label = 'Count'

    return p

def evolutionCharts(df):


    df['week']= df['snapshot'].apply(getWeekString)
    df=df.sort(['snapshot'], ascending=[1])
    source = ColumnDataSource(df)



    plots={'size':evolSize(source,df)}

    last=None
    for q in qa:
        pt = figure(   plot_width=600, plot_height=200
                , min_border=10, min_border_left=50
                , toolbar_location="above",responsive=True)
        pt.background_fill_color = "#fafafa"
        pt.legend.location = "top_left"
        pt.toolbar.logo = None
        pt.toolbar_location = None
        hit_renderers = []
        legends=[]
        print 'q',q
        for m,v in q['metrics'].items():
            print 'm',m
            print 'v',v
            l=pt.line(x='snapshot',y=m.lower(), line_width=2,source=source, color=v['color'])
            c=pt.circle(x='snapshot',y=m.lower(), line_width=2,source=source, color=v['color'])
            # invisible circle used for hovering
            hit_target =Circle(x='snapshot',y=m.lower(), size=10,line_color=None, fill_color=None)
            hit_renderer = pt.add_glyph(source, hit_target)

            #c.select(dict(type=HoverTool)).tooltips = {"Week": "@week",m:"@"+m.lower()}
            #hit_renderers.append(hit_renderer)

            legends.append((v['label']+" ["+m.lower()+"]",[l,c]))

            pt.add_tools(HoverTool(renderers=[hit_renderer], tooltips={'Metric':v['label'], "Week": "@week", 'Value':"@"+m.lower()}))
            pt.xaxis[0].ticker.desired_num_ticks = df.shape[0]

        pt.xaxis.formatter=FuncTickFormatter.from_py_func(getWeekString1)
        pt.axis.minor_tick_line_color = None

        legend = Legend(legends=legends, location=(0, -30))

        pt.add_layout(legend, 'right')

        pt.xaxis[0].axis_label = 'Snapshot'
        pt.yaxis[0].axis_label = 'Average quality'

        plots[q['dimension']]=pt
        last=pt
    print plots
    return plots