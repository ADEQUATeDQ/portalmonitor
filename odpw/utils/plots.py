from collections import defaultdict

import numpy as np
from bokeh.charts import Bar
from bokeh.layouts import column, row
from bokeh.models import NumeralTickFormatter, FuncTickFormatter, Text, Range1d, HoverTool, \
    ColumnDataSource, pd, Spacer, Circle, Legend, TextAnnotation, LinearAxis, GlyphRenderer
from bokeh.plotting import figure
from numpy.ma import arange

from odpw.utils.timing import Timer
from odpw.core.model import PortalSnapshotQuality, PortalSnapshot
from odpw.utils.utils_snapshot import getLastNSnapshots, getWeekString, getWeekString1
from bokeh.charts.attributes import cat, color
from bokeh.charts.operations import blend
import numpy as np
from bokeh.palettes import brewer



ex={}
ex['ExAc']={'label': 'Access', 'color':'#311B92'
                ,'description':'Does the meta data contain access information for the resources?'}
ex['ExCo']={'label': 'Contact', 'color':'#4527A0'
                ,'description':'Does the meta data contain information to contact the data provider or publisher?'}
ex['ExDa']={'label': 'Date', 'color':'#512DA8'
                ,'description':'Does the meta data contain information about creation and modification date of metadata and resources respectively?'}
ex['ExDi']={'label': 'Discovery', 'color':'#5E35B1'
                ,'description':'Does the meta data contain information that can help to discover/search datasets?'}
ex['ExPr']={'label': 'Preservation', 'color':'#673AB7'
                ,'description':'Does the meta data contain information about format, size or update frequency of the resources?'}
ex['ExRi']={'label': 'Rights', 'color':'#7E57C2'
                ,'description':'Does the meta data contain information about the license of the dataset or resource.?'}
ex['ExSp']={'label': 'Spatial', 'color':'#9575CD'
                ,'description':'Does the meta data contain spatial information?'}
ex['ExTe']={'label': 'Temporal', 'color':'#B39DDB'
                ,'description':'Does the meta data contain temporal information?'}
existence={'dimension':'Existence','metrics':ex, 'color':'#B39DDB'}

ac={}
ac['AcFo']={'label': 'Format', 'color':'#00838F'
    ,'description':'Does the meta data contain information that can help to discover/search datasets?'}
ac['AcSi']={'label': 'Size', 'color':'#0097A7'
    ,'description':'Does the meta data contain information that can help to discover/search datasets?'}
accuracy={'dimension':'Accurracy', 'metrics':ac, 'color':'#0097A7'}

co={}
co['CoAc']={'label': 'AccessURL', 'color':'#388E3C'
    ,'description':'Are the available values of access properties valid HTTP URLs?'}
co['CoCE']={'label': 'ContactEmail', 'color':'#1B5E20'
    ,'description':'Are the available values of contact properties valid emails?'}

co['CoCU']={'label': 'ContactURL', 'color':'#43A047'
    ,'description':'Are the available values of contact properties valid HTTP URLs?'}
co['CoDa']={'label': 'DateFormat', 'color':'#66BB6A'
    ,'description':'Is date information specified in a valid date format?'}
co['CoFo']={'label': 'FileFormat', 'color':'#A5D6A7'
    ,'description':'Is the specified file format or media type registered by IANA?'}
co['CoLi']={'label': 'License', 'color':'#C8E6C9'
    ,'description':'Can the license be mapped to the list of licenses reviewed by opendefinition.org?'}
conformance={'dimension':'Conformance', 'metrics':co, 'color':'#C8E6C9'}

op={}
op['OpFo']={'label': 'Format Openness', 'color':'#F4511E'
    ,'description':'Is the file format based on an open standard?'}
op['OpLi']={'label': 'License Openneness', 'color':'#FF8A65'
    ,'description':'s the used license conform to the open definition?'}
op['OpMa']={'label': 'Format machine readability', 'color':'#E64A19'
    ,'description':'Can the file format be considered as machine readable?'}
opendata={'dimension':'Open Data', 'metrics':op, 'color':'#E64A19'}

re={}
re['ReDa']={'label': 'Datasets', 'color':'#FF9800'}
re['ReRe']={'label': 'Resources', 'color':'#FFA726'}
retrievability={'dimension':'Retrievability', 'metrics':re, 'color':'#FFA726'}

qa=[existence, conformance, opendata]#, retrievability, accuracy]



def hm():
    m, s = divmod(tick, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)

    if d==0:
        return "%sh %sm"% (h, m)
    else:
        return "%sd %sh %sm"% (d,h, m)





def getFetchProcessChart(db, snapshot, n=3):

    data,cnts = getData(db, snapshot,n=n)

    return fetchProcessChart(data,cnts)

def getData(db, snapshot, n=3):
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
        #print len(dd)
        data[sn]=dd
    return data,cnts

def fetchProcessChart(data,cnts):

    bp = figure(plot_width=600, plot_height=300,y_axis_type="datetime",responsive=True,tools='')#,toolbar_location=None
    bp.toolbar.logo = None
    bp.toolbar_location = None

    bp.xaxis[0].formatter = NumeralTickFormatter(format="0.0%")
    bp.yaxis[0].formatter=FuncTickFormatter.from_py_func(hm)
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

        if sn == max(data.keys()):
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

    bp.x_range=Range1d(0, mx*1.2)
    bp.background_fill_color = "#fafafa"

    bp.legend.location = "top_left"

    return bp


def portalsScatter(df):
    df=df.fillna(0)

    def get_dataset(df, name):
        df1 = df[df['software'] == name].copy()
        #print name,df1.describe()
        del df1['software']
        return df1

    ckan = get_dataset(df,"CKAN")
    socrata = get_dataset(df,"Socrata")
    opendatasoft = get_dataset(df,"OpenDataSoft")
    all=df

    hmax = 0
    vmax = 0
    #print hmax, vmax

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
                        (ckan,'CKAN', '#3A5785')
                        ,(socrata,'Socrata', 'green')
                        ,(opendatasoft,'OpenDataSoft', 'red')
                        ]):

        s,l,c=item
        source=ColumnDataSource(data=s)
        p.scatter(x='datasetcount', y='resourcecount', size=3, source=source, color=c, legend=l)


        # create the horizontal histogram
        maxV= s['datasetcount'].max()
        bins= 10 ** np.linspace(np.log10(1), np.log10(maxV), 10)
        hhist, hedges = np.histogram(s['datasetcount'], bins=bins)#[0,5,10,50,100,500,1000,5000,10000,50000,100000]
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
        maxV= s['resourcecount'].max()
        bins= 10 ** np.linspace(np.log10(1), np.log10(maxV), 10)
        vhist, vedges = np.histogram(s['resourcecount'], bins=bins)#[0,5,10,50,100,500,1000,5000,10000,50000,100000]
        vzeros = np.zeros(len(vedges)-1)
        vmax = max(vhist)*1.5 if max(vhist)*1.5>vmax else vmax


        pv.ygrid.grid_line_color = None
        #pv.xaxis.major_label_orientation = np.pi/4
        pv.background_fill_color = "#fafafa"

        pv.quad(left=0, bottom=vedges[:-1], top=vedges[1:], right=vhist, color=c, line_color=c, alpha=0.5)
        vh1 = pv.quad(left=0, bottom=vedges[:-1], top=vedges[1:], right=vzeros, alpha=0.5, **LINE_ARGS)
        vh2 = pv.quad(left=0, bottom=vedges[:-1], top=vedges[1:], right=vzeros, alpha=0.1, **LINE_ARGS)

    ph.y_range=Range1d(0.1, hmax)
    pv.x_range=Range1d(0.1, vmax)

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
                         tooltips=[('quality value', '@value'), ('Metric', '@label'),('Dimension', '@Dimension'),('Percentage of datasets', '@perc')])
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

    l=p.line(x='snapshotId',y='datasetcount', line_width=2,source=source)
    c=p.circle(x='snapshotId',y='datasetcount', line_width=2,source=source)

    hit_target =Circle(x='snapshotId',y='datasetcount', size=10,line_color=None, fill_color=None)
    hit_renderer = p.add_glyph(source, hit_target)

    legends.append(("Datasets",[l,c]))
    p.add_tools(HoverTool(renderers=[hit_renderer], tooltips={'Metric':"Size", "Week": "@week", 'Value':"@datasetcount"}))

    #######
    l=p.line(x='snapshotId',y='resourcecount', line_width=2,source=source)
    c=p.circle(x='snapshotId',y='resourcecount', line_width=2,source=source)

    hit_target =Circle(x='snapshotId',y='resourcecount', size=10,line_color=None, fill_color=None)
    hit_renderer = p.add_glyph(source, hit_target)

    legends.append(("Resources",[l,c]))
    p.add_tools(HoverTool(renderers=[hit_renderer], tooltips={'Metric':"Size", "Week": "@week", 'Value':"@resourcecount"}))


    p.xaxis[0].ticker.desired_num_ticks = df.shape[0]/2

    p.xaxis.formatter=FuncTickFormatter.from_py_func(getWeekStringTick)
    p.axis.minor_tick_line_color = None

    legend = Legend( location=(0, -30))
    legend.items = legends
    p.add_layout(legend, 'right')

    p.xaxis[0].axis_label = 'Snapshot'
    p.yaxis[0].axis_label = 'Count'

    return p

def evolutionCharts(df):


    df['week']= df['snapshot'].apply(getWeekString)
    df = df[df['end'].notnull()]
    df=df.sort(['snapshot'], ascending=[1])
    df['snapshotId']= range(1, len(df) + 1)

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
        for m,v in q['metrics'].items():
            l=pt.line(x='snapshotId',y=m.lower(), line_width=2,source=source, color=v['color'])
            c=pt.circle(x='snapshotId',y=m.lower(), line_width=2,source=source, color=v['color'])
            # invisible circle used for hovering
            hit_target =Circle(x='snapshotId',y=m.lower(), size=10,line_color=None, fill_color=None)
            hit_renderer = pt.add_glyph(source, hit_target)

            legends.append((v['label']+" ["+m.lower()+"]",[l,c]))

            pt.add_tools(HoverTool(renderers=[hit_renderer], tooltips={'Metric':v['label'], "Week": "@week", 'Value':"@"+m.lower()}))
            pt.xaxis[0].ticker.desired_num_ticks = df.shape[0]/2


        pt.xaxis.formatter=FuncTickFormatter.from_py_func(getWeekStringTick)
        pt.axis.minor_tick_line_color = None

        legend = Legend(location=(0, -30))
        legend.items=legends
        pt.add_layout(legend, 'right')

        pt.xaxis[0].axis_label = 'Snapshot'
        pt.yaxis[0].axis_label = 'Average quality'

        plots[q['dimension']]=pt
        last=pt

    return plots

def getWeekStringTick():
    if tick is None or len(str(tick))==0:
        return ''
    year="'"+str(tick)[:2]
    week=int(str(tick)[2:])
    #d = d - timedelta(d.weekday())
    #dd=(week)*7
    #dlt = timedelta(days = dd)
    #first= d + dlt

    #dlt = timedelta(days = (week)*7)
    #last= d + dlt + timedelta(days=6)

    return 'W'+str(week)+'-'+str(year)
def systemEvolutionBarPlot(df, yLabel, values):
    with Timer(key='systemEvolutionBarPlot', verbose=True):
        p = Bar(df, label='snapshot', values=values, agg='sum', stack='software',
            legend='bottom_left', bar_width=0.5, xlabel="Snapshots", ylabel=yLabel, responsive=True, height=200,tools='hover')

        glyph_renderers = p.select(GlyphRenderer)
        bar_source = [glyph_renderers[i].data_source for i in range(len(glyph_renderers))]
        hover = p.select(HoverTool)
        hover.tooltips = [
            ('software',' @software'),
            ('value', '@height'),
        ]
        p.xaxis.formatter=FuncTickFormatter.from_py_func(getWeekStringTick)
        p.axis.minor_tick_line_color = None

        p.background_fill_color = "#fafafa"
        p.legend.location = "top_left"
        p.toolbar.logo = None
        p.toolbar_location = None

        legend=p.legend[0].legends
        p.legend[0].legends=[]
        l = Legend( location=(0, -30))
        l.items=legend
        p.add_layout(l, 'right')

        return p

def systemEvolutionPlot(df):
    df=df.sort(['snapshot','count'], ascending=[1,0])

    p= systemEvolutionBarPlot(df,yLabel="#Portals", values='count')
    pd= systemEvolutionBarPlot(df,yLabel="#Datasets", values='datasets')
    pr= systemEvolutionBarPlot(df,yLabel="#Resources", values='resources')

    return {'portals':p,'datasets':pd,'resources':pr}

def portalDynamicity(df):

    def getWeekString(yearweek):
        if yearweek is None or len(str(yearweek)) == 0:
            return ''
        year = "'" + str(yearweek)[:2]
        week = int(str(yearweek)[2:])
        # d = d - timedelta(d.weekday())
        # dd=(week)*7
        # dlt = timedelta(days = dd)
        # first= d + dlt

        # dlt = timedelta(days = (week)*7)
        # last= d + dlt + timedelta(days=6)

        return 'W' + str(week) + '-' + str(year)
    bp = figure(plot_width=600, plot_height=300, y_axis_type="datetime", responsive=True,
                tools='')  # ,toolbar_location=None
    bp.toolbar.logo = None
    bp.toolbar_location = None
    label_dict={}
    for i, s in enumerate(df['snapshot']):
        label_dict[i] = getWeekString1(s)

    bp.yaxis[0].formatter = NumeralTickFormatter(format="0.0%")
    bp.xaxis[0].axis_label = 'Snapshots'
    bp.yaxis[0].axis_label = '% of portals'

    li = bp.line(df.index.values.tolist(), df['dyratio'], line_width=2, line_color='red', legend="dyratio")
    c = bp.circle(df.index.values.tolist(), df['dyratio'], line_width=2, line_color='red', legend="dyratio")
    li1 = bp.line(df.index.values.tolist(), df['adddelratio'], line_width=2, line_color='blue', legend="adddelratio")
    c = bp.circle(df.index.values.tolist(), df['adddelratio'], line_width=2, line_color='blue', legend="adddelratio")
    legend = bp.legend[0].legends
    bp.legend[0].legends = []
    l = Legend(location=(0, -30))
    l.items = legend
    bp.add_layout(l, 'right')



    labels=["staticRatio","updatedRatio","addRatio","delRatio"]
    #for l in labels:
    #    df[l]= df[l]*100
    print brewer.keys()
    colors = brewer["Pastel2"][len(labels)]
    bar = Bar(df,
              values=blend("staticRatio","updatedRatio","addRatio","delRatio", name='medals', labels_name='medal'),
              label=cat(columns='snapshot', sort=False),
              stack=cat(columns='medal', sort=False),
              color=color(columns='medal', palette=colors,
                          sort=False),
              legend='top_right',
              bar_width=0.5, responsive=True,
              tooltips=[('ratio', '@medal'), ('snapshot', '@snapshot'),('Value of Total',' @height{0.00%}')])
    legend = bar.legend[0].legends
    bar.legend[0].legends = []
    l = Legend(location=(0, -30))
    l.items = legend
    bar.add_layout(l, 'right')
    bar.xaxis[0].axis_label = 'Snapshots'
    bar.yaxis[0].axis_label = '% of datasets'
    bar.width=600
    bar.height=300
    bar.xaxis[0].formatter = FuncTickFormatter.from_py_func(getWeekStringTick)
    bar.toolbar.logo = None
    bar.toolbar_location = None

    bar.yaxis[0].formatter = NumeralTickFormatter(format="0.0%")
    return {'bar':bar,'lines':bp}