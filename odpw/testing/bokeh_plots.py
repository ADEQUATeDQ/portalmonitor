'''
Created on Dec 20, 2015

@author: jumbrich
'''


    
from collections import OrderedDict
from math import log, sqrt


import numpy as np
import pandas as pd
from six.moves import cStringIO as StringIO

from bokeh.plotting import figure, show, output_file
from bokeh.embed import components

from bokeh.models import HoverTool
import bokeh.models as bkm
import bokeh.plotting as bkp
from bokeh.models.sources import ColumnDataSource
from bokeh.models import widgets
from bokeh.io import output_file, show, vform

def line():
    
    
    x = [1, 2, 3, 4, 5]
    ys = [[6, 7, 2, 4, 5], [5, 4, 2, 7, 6]]
    
    hover = HoverTool(
     tooltips=[
     ("(x,y)", "($x, $y)"),
     ("label", "@label"),
     ]
    )
    
    output_file("test_bokeh.html", title="bokeh feature test")
    
    p = figure(title='figure', x_axis_label='x', y_axis_label='y', tools=[hover])
    line_source = ColumnDataSource({
     'x': x,
     'y': x,
     'label': ['single line'] * 5,
    })
    p.line('x', 'x', source=line_source)
    multi_line_source = ColumnDataSource({
     'xs': [x, x],
     'ys': ys,
     'label': ['line 0', 'line_1'],
     'color': ['red', 'blue'],
    })
    p.multi_line('xs', 'ys', color='color', source=multi_line_source)
    
    show(p)



def areal1():
    d=[]
    dim_color = {}
    key_color = {}
    for inD in qa:
        for k,v in inD['metrics'].items():
            c= {'key':k, 'dim':inD['dimension'], 'value':1}
            c.update(v)
            d.append(c)
            dim_color[inD['dimension']] =inD['color']
            key_color[k]= v['color'] 
    
    df = pd.DataFrame(d)
    print df
    width = 800
    height = 800
    
    inner_radius = 90
    outer_radius = 300 - 10
    
    minr = 0 #sqrt(log(0 * 1E4))
    maxr = 1#sqrt(log(1 * 1E4))
    a = (outer_radius - inner_radius) / (maxr - minr)
    b = inner_radius
    
    #===========================================================================
    # def rad(value):
    #     print value
    #     # Figure out how 'wide' each range is
    #     valueSpan = 1 - 0
    #     radiusSpan = outer_radius - inner_radius
    #     
    #     # Convert the left range into a 0-1 range (float)
    #     valueScaled = (value - 0) / float(radiusSpan)
    #     print valueScaled
    #     return valueScaled
    #===========================================================================
    
    def rad(mic):
        v = a * mic + b
        return v
    
    big_angle = 2.0 * np.pi / (len(df) + 1)
    small_angle = big_angle / 7
    
    x = np.zeros(len(df))
    y = np.zeros(len(df))
    
    output_file("qa_spie.html", title="qa_spie.py example")

    # create chart
    TOOLS = "hover"
    p = figure(plot_width=width, plot_height=height, title="",
        x_axis_type=None, y_axis_type=None,
        x_range=[-420, 420], y_range=[-420, 420],
        min_border=0, outline_line_color="black",
        background_fill="#f0e1d2", border_fill="#f0e1d2", tools=TOOLS)
    
    p.line(x+1, y+1, alpha=0.5)
    
    # annular wedges
    angles = np.pi/2 - big_angle/2 - df.index.to_series()*big_angle
    colors = [dim_color[dim] for dim in df.dim]
    p.annular_wedge(
        x, y, 300, 315, -big_angle+angles, angles, color=colors,
    )    
    
    source = ColumnDataSource(df)
    kcolors = [key_color[k] for k in df.key]
    p.annular_wedge(x, y, inner_radius, rad(df.value),
        -big_angle+angles+1*small_angle, -big_angle+angles+6*small_angle,
        color=kcolors, source=source)
    
    
    labels = np.array([c / 100.0 for c in range(0, 100, 10)]) #
    radii = a * labels + b
    
    p.circle(x, y, radius=radii, fill_color=None, line_color="white")
    p.text(x[:-1], radii[:-1], [str(r) for r in labels[:-1]],
        text_font_size="8pt", text_align="center", text_baseline="middle")
    
    # radial axes
    p.annular_wedge(x, y, inner_radius-10, outer_radius+10,
        -big_angle+angles, -big_angle+angles, color="black")
    
    # bacteria labels
    xr = radii[5]*np.cos(np.array(-big_angle/2 + angles))
    yr = radii[5]*np.sin(np.array(-big_angle/2 + angles))
    print "xr",xr
    label_angle=np.array(-big_angle/2+angles)
    label_angle[label_angle < -np.pi/2] += np.pi # easier to read labels on the left side
    p.text(xr, yr, df.label, angle=label_angle,
        text_font_size="9pt", text_align="center", text_baseline="middle")
    
    
    #dim legend
    p.rect([-40,-40, -40, -40,-40], [36,18, 0, -18, -36], width=30, height=13,
        color=list(dim_color.values()))
    p.text([-15,-15, -15, -15,-15], [36,18, 0, -18,-36], text=list(dim_color.keys()),
        text_font_size="9pt", text_align="left", text_baseline="middle")
    
    hover = p.select(dict(type=HoverTool))
    hover.tooltips = [
                      ('value', '@value'),
                      ('label','@label')
                      ]
    
    columns = [
               widgets.TableColumn(field=c, title=c) for c in df.columns
    ]
    print columns
    data_table = widgets.DataTable(source=source, columns=columns, editable=True, width = 500)
    
    f = vform(p,data_table)
    js, divs = components({"data": data_table, "graph": p})
    show(f)
    #show(p)

def areal():
    
    antibiotics = """
    bacteria,                        penicillin, streptomycin, neomycin, gram
    Mycobacterium tuberculosis,      800,        5,            2,        negative
    Salmonella schottmuelleri,       10,         0.8,          0.09,     negative
    Proteus vulgaris,                3,          0.1,          0.1,      negative
    Klebsiella pneumoniae,           850,        1.2,          1,        negative
    Brucella abortus,                1,          2,            0.02,     negative
    Pseudomonas aeruginosa,          850,        2,            0.4,      negative
    Escherichia coli,                100,        0.4,          0.1,      negative
    Salmonella (Eberthella) typhosa, 1,          0.4,          0.008,    negative
    Aerobacter aerogenes,            870,        1,            1.6,      negative
    Brucella antracis,               0.001,      0.01,         0.007,    positive
    Streptococcus fecalis,           1,          1,            0.1,      positive
    Staphylococcus aureus,           0.03,       0.03,         0.001,    positive
    Staphylococcus albus,            0.007,      0.1,          0.001,    positive
    Streptococcus hemolyticus,       0.001,      14,           10,       positive
    Streptococcus viridans,          0.005,      10,           40,       positive
    Diplococcus pneumoniae,          0.005,      11,           10,       positive
    """
    
    drug_color = OrderedDict([
        ("Penicillin",   "#0d3362"),
        ("Streptomycin", "#c64737"),
        ("Neomycin",     "black"  ),
    ])
    
    gram_color = {
        "positive" : "#aeaeb8",
        "negative" : "#e69584",
    }
    
    df = pd.read_csv(StringIO(antibiotics),
                     skiprows=1,
                     skipinitialspace=True,
                     engine='python')
    
    width = 800
    height = 800
    inner_radius = 90
    outer_radius = 300 - 10
    
    minr = sqrt(log(.001 * 1E4))
    maxr = sqrt(log(1000 * 1E4))
    a = (outer_radius - inner_radius) / (minr - maxr)
    b = inner_radius - a * maxr
    
    def rad(mic):
        return a * np.sqrt(np.log(mic * 1E4)) + b
    
    big_angle = 2.0 * np.pi / (len(df) + 1)
    small_angle = big_angle / 7
    
    x = np.zeros(len(df))
    y = np.zeros(len(df))
    
    print x
    print y
    
    output_file("burtin.html", title="burtin.py example")
    
    p = figure(plot_width=width, plot_height=height, title="",
        x_axis_type=None, y_axis_type=None,
        x_range=[-420, 420], y_range=[-420, 420],
        min_border=0, outline_line_color="black",
        background_fill="#f0e1d2", border_fill="#f0e1d2")
    
    p.line(x+1, y+1, alpha=0)
    
    # annular wedges
    angles = np.pi/2 - big_angle/2 - df.index.to_series()*big_angle
    colors = [gram_color[gram] for gram in df.gram]
    p.annular_wedge(
        x, y, inner_radius, outer_radius, -big_angle+angles, angles, color=colors,
    )
    
    # small wedges
    p.annular_wedge(x, y, inner_radius, rad(df.penicillin),
        -big_angle+angles+5*small_angle, -big_angle+angles+6*small_angle,
        color=drug_color['Penicillin'])
    p.annular_wedge(x, y, inner_radius, rad(df.streptomycin),
        -big_angle+angles+3*small_angle, -big_angle+angles+4*small_angle,
        color=drug_color['Streptomycin'])
    p.annular_wedge(x, y, inner_radius, rad(df.neomycin),
        -big_angle+angles+1*small_angle, -big_angle+angles+2*small_angle,
        color=drug_color['Neomycin'])
    
    # circular axes and lables
    labels = np.power(10.0, np.arange(-3, 4))
    radii = a * np.sqrt(np.log(labels * 1E4)) + b
    p.circle(x, y, radius=radii, fill_color=None, line_color="white")
    p.text(x[:-1], radii[:-1], [str(r) for r in labels[:-1]],
        text_font_size="8pt", text_align="center", text_baseline="middle")
    
    # radial axes
    p.annular_wedge(x, y, inner_radius-10, outer_radius+10,
        -big_angle+angles, -big_angle+angles, color="black")
    
    # bacteria labels
    xr = radii[0]*np.cos(np.array(-big_angle/2 + angles))
    yr = radii[0]*np.sin(np.array(-big_angle/2 + angles))
    label_angle=np.array(-big_angle/2+angles)
    label_angle[label_angle < -np.pi/2] += np.pi # easier to read labels on the left side
    p.text(xr, yr, df.bacteria, angle=label_angle,
        text_font_size="9pt", text_align="center", text_baseline="middle")
    
    # OK, these hand drawn legends are pretty clunky, will be improved in future release
    p.circle([-40, -40], [-370, -390], color=list(gram_color.values()), radius=5)
    p.text([-30, -30], [-370, -390], text=["Gram-" + gr for gr in gram_color.keys()],
        text_font_size="7pt", text_align="left", text_baseline="middle")
    
    p.rect([-40, -40, -40], [18, 0, -18], width=30, height=13,
        color=list(drug_color.values()))
    p.text([-15, -15, -15], [18, 0, -18], text=list(drug_color.keys()),
        text_font_size="9pt", text_align="left", text_baseline="middle")
    
    p.xgrid.grid_line_color = None
    p.ygrid.grid_line_color = None
    
    script, div = components(p)
    print script
    print div
    show(p)

if __name__ == '__main__':
    areal1()
    #areal()